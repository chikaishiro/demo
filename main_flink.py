#flink版本是1.10.0， pyflink（pip install apache-flink）版本是1.10.*
import cut_posseg
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.udf import udf
from pyflink.datastream import StreamExecutionEnvironment

def main_flink():
    #之前的步骤是拿文件然后预处理然后写文件到input这个文件中
    env = StreamExecutionEnvironment.get_execution_environment()
    parr_num = 4
    env.set_parallelism(parr_num)
    t_env = StreamTableEnvironment.create(env)
    @udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())
    def cut_extract(string):
        return cut_posseg.cut_extract(string)


    t_env.register_function("cut_extract",cut_extract)
    #这里是建表然后从input拿，问题1：有没有办法从自定义的list来当做输入来节省IO开销呢，比如我输入[文本A，文本B]这样的list作为输入
    t_env.connect(FileSystem().path('/home/sjtu/input')) \
        .with_format(OldCsv()
                     .field('text', DataTypes.STRING())) \
        .with_schema(Schema()
                     .field('text', DataTypes.STRING())) \
        .create_temporary_table('mySource')

    t_env.connect(FileSystem().path('/home/sjtu/output')) \
        .with_format(OldCsv()
                     .field('result', DataTypes.STRING())) \
        .with_schema(Schema()
                     .field('result', DataTypes.STRING())) \
        .create_temporary_table('mySink')

    t_env.from_path('mySource')\
        .select("cut_extract(text)")\
        .insert_into('mySink')
    #问题2：这里我是将结果的表写了文件，但实际上我还需要在这个代码中继续对这些处理完的数据进行处理，也没有办法直接将上述的mySink表直接
    #作为内存数据取出来而不是从硬盘再读入呢
    t_env.execute("tutorial_job")
    #问题3：我现在看文档是只能用这种方式使用python的自定义函数（使用StreamExecutionEnvironment和tableAPI），还有其他更好的方法可以完成
    #这样一个流程吗
    #问题4：这样设置并行度为4会导致输出的为output一个文件夹，里面有output/1 output/2 output/3 output/4这四个文件，然后我之后的处理方法是
    #按parr_num数重新读文件把他们合并，还有更好的方法吗
    #问题5：一整个流程涉及多次本地文件系统IO操作，这样做假如放到集群上会出问题吗
if __name__ == "__main__":
    main_flink()

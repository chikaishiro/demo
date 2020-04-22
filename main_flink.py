def main_flink():
    args = parser.parse_args()
    file_list = ["json_data/json-0.txt"]
    sentence_list = get_weibo_data_from_json_txt(file_list)
    cleaned_sentences = clean_weibo_data(sentence_list)
    print("total data size: " + str(len(cleaned_sentences)))

    env = StreamExecutionEnvironment.get_execution_environment()
    parr_num = 4
    env.set_parallelism(parr_num)
    t_env = StreamTableEnvironment.create(env)
    @udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())
    def cut_extract(string):
        return cut_posseg.cut_extract(string)


    t_env.register_function("cut_extract",cut_extract)
    #t_env.register_function("add", udf(lambda i: i, DataTypes.STRING, DataTypes.STRING()))
    t_env.connect(FileSystem().path('/home/sjtuadm/hotspot/input_json')) \
        .with_format(OldCsv()
                     .field('text', DataTypes.STRING())) \
        .with_schema(Schema()
                     .field('text', DataTypes.STRING())) \
        .create_temporary_table('mySource')

    t_env.connect(FileSystem().path('/home/sjtuadm/hotspot/output')) \
        .with_format(OldCsv()
                     .field('result', DataTypes.STRING())) \
        .with_schema(Schema()
                     .field('result', DataTypes.STRING())) \
        .create_temporary_table('mySink')

    t_env.from_path('mySource')\
        .select("cut_extract(text)")\
        .insert_into('mySink')

    t_env.execute("tutorial_job")

if __name__ = "__main__":
    main_flink()

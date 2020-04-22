def get_tag_from_sentence(sentence):
    pattern = re.compile('#.*?#', re.S)
    result = pattern.findall(sentence)
    single_tag_list = []
    for tag in result:
        tag = tag[1:-1]
        single_tag_list.append(tag)
    sorted_list = sorted(single_tag_list, key=lambda d: len(d), reverse=True)
    tag = ""
    try:
        tag = sorted_list[0]
    except:
        tag = ""
    return tag
    
def stopwordslist(stopwords_file):
    stopwords = [line.strip() for line in open(stopwords_file,encoding='UTF-8').readlines()]
    return stopwords

def cut_extract(sentence):
    stopwords = stopwordslist("stopwords.txt")
    tag = get_tag_from_sentence(sentence)
    sentence = re.sub(r'[^\u4e00-\u9fa5]+', '', sentence)
    sentence_depart = jieba.posseg.cut(sentence.strip())
    outstr = ""
    for word in sentence_depart:
        if word.flag not in ['d','p','t','x','r','a','b','m','c','vg','u','nj'] and len(word.word) > 1 and word not in stopwords:
            outstr += word.word
            outstr += " "
    return outstr + ";" + tag

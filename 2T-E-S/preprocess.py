# encoding=utf-8
import re
import jieba.posseg as pseg

def weibo_text_processing(text, stopwords):
    # 正则表达式处理
    patterns = [
        (r"<(?!\/)a.*?>|<(\/)a.*?>|<(?!\/)a.*?>.*<(\/)a.*?>|<br />", ""),  # 去除a标签和换行符
        (r"<span.*?>(.*?)<\/span>", r"\1"),  # 保留span标签内容
        (r"#[^#]+#", ""),  # 去除话题标签
        (r"(\/\/@.*?:.*)|(\/\/@.*?：.*)|(@.*?:)|(@.*?：)", ""),  # 去除@引用
        (r"(\ud83c[\udf00-\udfff])|(\ud83d[\udc00-\ude4f\ude80-\udeff])|[\u2600-\u2B55]", ""),  # 去除emoji和符号
        (r"@[^\s]+[^\s]*的微博视频", "")  # 去除@...微博视频
    ]

    # 应用所有正则规则
    for pattern, repl in patterns:
        text = re.sub(pattern, repl, text, flags=re.UNICODE)

    # 使用jieba进行分词和词性标注（启用paddle模式）
    words = pseg.cut(text, use_paddle=True)

    # 筛选词性并去停用词
    allow_pos = {'n', 'nr', 'ns', 'nt', 'vn'}  # 名词、专有名词、动名词
    result = [
        word.word for word in words
        # if word.flag in allow_pos
        #    and word.word not in stopwords
        #    and len(word.word.strip()) > 0
        if word.word not in stopwords
           and len(word.word.strip()) > 0
    ]

    return result

# 使用示例
if __name__ == "__main__":
    # 示例停用词表（需替换为实际停用词文件）
    with open('cn_stopwords.txt', 'r', encoding='utf-8') as f:
        stopwords = set([line.strip() for line in f])
    # 示例文本
    sample_text = '''
【#村民抗议镇中学合并到县城中学#，“都是留守儿童，没有经济能力去城里读”】2月10日，四川宜宾。网曝筠连县丰乐乡龙镇中心校初中部突然宣布要合并到县城中学，村民拉横幅反对。视频中，多名村民在校门口拉横幅，横幅上有“反对撤销龙镇初中部，支持就近上学”的字样，抗议让学生去县城上学。2月11日，村民陈女士表示，“这边是山区，离县城有25公里，而且交通不便。我们这里很多都是五六个孩子，还有七个孩子的，最少是三个，像我们家五个娃都是留守儿童，都是老人在家照顾，年轻人出门打工，老人字都不认识几个，也不会开车。附近几个村的孩子都在此学校读书，有几百个学生。之前就已经把村里的学校撤到镇上了，没钱坐车的孩子去上学，都是步行两个小时。现在又要撤到县中学，因为这个撤校的事，有的家长不让孩子读书了，我们家没有经济能力负担孩子在县城读书。”2月11日，筠连县教育局工作人员表示：“现在领导都商议，在处理中，有消息会通知。”@猫头鹰视频L猫头鹰视频的微博视频
    '''

    processed = weibo_text_processing(sample_text, stopwords)
    print(processed)
    # 预期输出可能包含：['测试链接', 'span', '内容', '换行', '内容']
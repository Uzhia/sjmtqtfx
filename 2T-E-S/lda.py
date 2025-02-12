from gensim import corpora
from gensim.models import LdaModel

import re
import jieba.posseg as pseg
import pandas as pd

def weibo_text_processing(text, stopwords):
    # 正则表达式处理
    patterns = [
        (r"<(?!\/)a.*?>|<(\/)a.*?>|<(?!\/)a.*?>.*<(\/)a.*?>|<br />", ""),  # 去除a标签和换行符
        (r"<span.*?>(.*?)<\/span>", r"\1"),  # 保留span标签内容
        (r"#[^#]+#", ""),  # 去除话题标签
        (r"(\/\/@.*?:.*)|(\/\/@.*?：.*)|(@.*?:)|(@.*?：)", ""),  # 去除@引用
        (r"(\ud83c[\udf00-\udfff])|(\ud83d[\udc00-\ude4f\ude80-\udeff])|[\u2600-\u2B55]", "")  # 去除emoji和符号
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
        if word.flag in allow_pos
           and word.word not in stopwords
           and len(word.word.strip()) > 0
    ]

    return result
if __name__ == "__main__":
    texts = []
    # 示例停用词表（需替换为实际停用词文件）
    with open('cn_stopwords.txt', 'r', encoding='utf-8') as f:
        stopwords = set([line.strip() for line in f])
    import pymysql

    # 数据库连接信息
    db_config = {
        'host': 'localhost',  # 数据库服务器地址
        'user': 'root',  # 数据库用户名
        'password': '6',  # 数据库密码
        'database': 'weibo',  # 数据库名称
        'charset': 'utf8mb4',  # 字符集
        'cursorclass': pymysql.cursors.DictCursor  # 使用字典形式返回结果
    }

    # 连接到数据库
    connection = pymysql.connect(**db_config)

    try:
        # 创建游标对象
        with connection.cursor() as cursor:
            # 查询语句
            sql = "SELECT `text` FROM `weibo0212`"
            cursor.execute(sql)

            # 获取查询结果
            results = cursor.fetchall()

            # 提取所有文本内容
            texts_from_db = [row['text'] for row in results if row['text'] is not None]

    finally:
        # 关闭数据库连接
        connection.close()

    i = 1
    for text in texts_from_db:
        processed = weibo_text_processing(text, stopwords)
        if i == 1:
            print(processed)
            i += 1
        # 预期输出可能包含：['测试链接', 'span', '内容', '换行', '内容']
        texts.append(processed)


    # 创建词典
    dictionary = corpora.Dictionary(texts)
    dictionary.filter_extremes(no_below=2, no_above=0.8)  # 过滤低频和高频词

    # 将分词后的文本转换为词典中的词 ID
    corpus = [dictionary.doc2bow(text) for text in texts]

    # 训练 LDA 模型
    lda_model = LdaModel(
        corpus=corpus,
        id2word=dictionary,
        num_topics=10,   # 设置主题数目
        passes=1000       # 迭代次数
    )

    # 获取每个文档的主题分布
    document_topics = lda_model.get_document_topics(corpus)

    # 确定每个文档的主导主题
    dominant_topics = []
    topic_distributions = []

    for doc_topics in document_topics:
        dominant_topic = max(doc_topics, key=lambda x: x[1])[0]  # 获取概率最高的主题ID
        dominant_topics.append(dominant_topic)
        topic_distributions.append(doc_topics)

    # 提取每个主题的前5个主题词
    topic_keywords = {}
    for topic_id in range(lda_model.num_topics):
        topic_words = lda_model.show_topic(topic_id, topn=5)
        topic_keywords[topic_id] = ", ".join([word for word, prop in topic_words])

    # 创建一个DataFrame来存储结果
    data = {
        '原始文本': texts_from_db,
        '主导主题': dominant_topics,
        '主题分布': topic_distributions,
        '主题关键词': [topic_keywords[topic_id] for topic_id in dominant_topics]
    }

    df = pd.DataFrame(data)

    # 将DataFrame导出为CSV文件
    df.to_csv('lda_results.csv', index=False, encoding='utf-8-sig')

    print("结果已成功导出到 'lda_results.csv' 文件中。")

    # 输出主题
    # for idx, topic in lda_model.print_topics(-1):
    #     print(f"Topic {idx}: {topic}")
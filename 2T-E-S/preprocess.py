import re
import jieba.posseg as pseg

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


# 使用示例
if __name__ == "__main__":
    # 示例停用词表（需替换为实际停用词文件）
    with open('cn_stopwords.txt', 'r', encoding='utf-8') as f:
        stopwords = set([line.strip() for line in f])
    # 示例文本
    sample_text = '''
#乌克兰决定延长国家战时状态 90 天#】新华社基辅 5 月 18 日电
乌克兰最高拉达（议会）18 日在其官网发布了关于批准延长国家战
时状态总统令草案的消息。根据新的总统令，乌克兰的国家战时状
态将于 5 月 25 日 5 时 30 分到期后一次性延长 90 天。<br/>
    '''

    processed = weibo_text_processing(sample_text, stopwords)
    print(processed)
    # 预期输出可能包含：['测试链接', 'span', '内容', '换行', '内容']
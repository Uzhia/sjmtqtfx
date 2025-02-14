# encoding=utf-8
from time import sleep
import pymysql, json, csv, re, requests, os
from tqdm import tqdm  # 用于进度条显示
from datetime import datetime


# 数据库配置
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "6",
    "database": "weibo",
    "charset": "utf8mb4",
    "port": 3306
}

# API 配置
api_url = "https://api.gpts.vin/v1/chat/completions"
api_key = "sk-"

# 清理文本的正则表达式
patterns = [
    (r"<(?!\/)a.*?>|<(\/)a.*?>|<(?!\/)a.*?>.*<(\/)a.*?>|<br />", ""),
    (r"<span.*?>(.*?)</span>", r"\1"),
    (r"#[^#]+#", ""),
    (r"(\/\/@.*?:.*)|(\/\/@.*?：.*)|(@.*?:)|(@.*?：)", ""),
    (r"(\ud83c[\udf00-\udfff])|(\ud83d[\udc00-\ude4f\ude80-\udeff])|[\u2600-\u2B55]", ""),
    (r"@[^\s]+[^\s]*的微博视频", "")
]

# 用于记录已处理的评论 ID
processed_comments = "processed_comments.csv"


def connect_db(config):
    try:
        return pymysql.connect(**config)
    except pymysql.MySQLError as e:
        print(f"数据库连接失败：{e}")
        return None


def fetch_comments_with_weibo(conn, processed_ids):
    try:
        with conn.cursor() as cursor:
            # 如果 processed_ids 为空，跳过 IN 子句
            if not processed_ids:
                sql = """
                SELECT c.id AS comment_id, c.text AS comment_text, w.text AS weibo_text 
                FROM comments0212 c
                JOIN weibo0212 w ON c.weibo_id = w.id
                LEFT JOIN weibo_event0212 we ON w.id = we.id
                WHERE c.text IS NOT NULL 
                  AND w.text IS NOT NULL 
                  AND (we.event_id != 8 OR we.event_id IS NULL)
                """
            else:
                sql = f"""
                SELECT c.id AS comment_id, c.text AS comment_text, w.text AS weibo_text 
                FROM comments0212 c
                JOIN weibo0212 w ON c.weibo_id = w.id
                LEFT JOIN weibo_event0212 we ON w.id = we.id
                WHERE c.text IS NOT NULL 
                  AND w.text IS NOT NULL 
                  AND c.id NOT IN ({",".join([f"'{id}'" for id in processed_ids])})
                  AND (we.event_id != 8 OR we.event_id IS NULL)
                """
            cursor.execute(sql)
            return cursor.fetchall()
    except pymysql.MySQLError as e:
        print(f"数据库查询失败：{e}")
        return []


def clean_text(text):
    for pattern, repl in patterns:
        text = re.sub(pattern, repl, text, flags=re.UNICODE)
    return text.strip()


def analyze_sentiment(weibo_content, comment_content):
    data = {}
    try:

        prompt = """你是一个情感分析工具。请将对于特定微博的评论转化为 [0,1] 的立场值，规则：
- 负面[0,0.33)：表达愤怒/失望
- 中立[0.33,0.66)：无倾向或混合评价
- 积极[0.66,1]：明显赞扬
直接返回数值（2位小数），格式：数值。
""".strip()

        user_content = f"""微博：“{weibo_content}”
评论：“{comment_content}”
""".strip()
        data = {
            "model": "gpt-3.5-turbo",
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_content}
            ],
            "temperature": 0.1
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        response = requests.post(api_url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()['choices'][0]['message']['content']
    except Exception as e:
        data["error"] = f"API请求失败：{e}"
        # 将 data 写入文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"request_data_{timestamp}.json"

        if not os.path.exists("requests"):
            os.makedirs("requests")

        with open(f"requests/{filename}", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return None


def save_processed_comment(comment_id):
    with open(processed_comments, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([comment_id])


def load_processed_comments():
    try:
        with open(processed_comments, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            return [row[0] for row in reader]
    except FileNotFoundError:
        return []


def main():
    conn = connect_db(db_config)
    if not conn:
        return

    # 加载已处理的评论 ID
    processed_ids = load_processed_comments()
    data = fetch_comments_with_weibo(conn, processed_ids)
    processed_count = 0  # 用于进度条

    # 打开 CSV 文件用于追加写入，并提前创建 writer 对象
    # 检查文件是否存在，如果不存在则写入表头
    import os
    write_header = not os.path.exists("4_sentiment_results.csv")
    with open("4_sentiment_results.csv", "a", encoding="utf-8", newline="") as f:
        fieldnames = ["comment_id", "sentiment"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        # 如果文件不存在，写入表头
        if write_header:
            writer.writeheader()

        # 使用 tqdm 显示进度条
        for item in tqdm(data, desc="Processing Comments", unit="comment"):
            comment_id, comment_text, weibo_text = item

            cleaned_weibo = clean_text(weibo_text)[:500]
            cleaned_comment = clean_text(comment_text)[:500]

            sentiment = analyze_sentiment(cleaned_weibo, cleaned_comment)
            wait_time = 0
            while sentiment is None:
                wait_time += 3
                sleep(wait_time)
                sentiment = analyze_sentiment(cleaned_weibo, cleaned_comment)

            # 如果 sentiment 为 None，写入 "Bad Request"
            if sentiment is None:
                sentiment_value = "Bad Request"
            elif re.match(r"^\d\.\d{2}$", sentiment):
                sentiment_value = float(sentiment)
            else:
                sentiment_value = "解析失败"

            # 立即将结果写入 CSV 文件
            writer.writerow({
                "comment_id": comment_id,
                "sentiment": sentiment_value
            })

            # 保存已处理的评论 ID 到文件
            save_processed_comment(comment_id)
            processed_count += 1

    conn.close()


if __name__ == "__main__":
    main()


"""
你是一个情感分析工具。请将对于特定微博的评论转化为 [0,1] 的立场值，规则：
- 负面[0,0.33)：表达愤怒/失望
- 中立[0.33,0.66)：无倾向或混合评价
- 积极[0.66,1]：明显赞扬
直接返回数值（2位小数），格式：数值。
微博：“”
评论：“”

"""
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
processed_comments_file_name = "processed_comments_kimi.csv"


def connect_db(config):
    try:
        return pymysql.connect(**config)
    except pymysql.MySQLError as e:
        print(f"数据库连接失败：{e}")
        return None

def load_processed_comments():
    try:
        with open(processed_comments_file_name, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            return [row[0] for row in reader]
    except FileNotFoundError:
        return []


def fetch_comments_with_weibo_kimi(conn, report_file_path):
    try:
        # 读取 report.txt 文件中的评论 ID
        with open(report_file_path, 'r') as file:
            processing_ids = [line.strip() for line in file if line.strip()]

        if not processing_ids:
            print("report.txt 文件是空的或没有有效内容")
            return []

        processed_ids = load_processed_comments()

        result = [item for item in processing_ids if item not in processed_ids]
        with conn.cursor() as cursor:
            # 使用参数化查询以避免 SQL 注入
            placeholders = ', '.join(['%s'] * len(result))
            sql = f"""
            SELECT c.id AS comment_id, c.text AS comment_text, w.text AS weibo_text 
            FROM comments0212 c
            JOIN weibo0212 w ON c.weibo_id = w.id
            LEFT JOIN weibo_event0212 we ON w.id = we.id
            WHERE c.text IS NOT NULL 
              AND w.text IS NOT NULL 
              AND c.id IN ({placeholders})
              AND (we.event_id != 8 OR we.event_id IS NULL)
            """
            cursor.execute(sql, tuple(result))
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
        from openai import OpenAI

        client = OpenAI(
            api_key="sk-",
            base_url="https://api.moonshot.cn/v1",
        )

        completion = client.chat.completions.create(
            model="moonshot-v1-8k",
            messages=[
                {"role": "system",
                 "content": prompt},
                {"role": "user", "content": user_content}
            ],
            temperature=0.3,
        )
        return completion.choices[0].message.content
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
    with open(processed_comments_file_name, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([comment_id])


def main():
    # 在4_sentiment_results中值为解析错误的评论中，再通过kimi处理一下
    file_name = "5_sentiment_results_kimi.csv"
    conn = connect_db(db_config)
    if not conn:
        return

    data = fetch_comments_with_weibo_kimi(conn, "4_report.txt")
    processed_count = 0  # 用于进度条

    # 打开 CSV 文件用于追加写入，并提前创建 writer 对象
    # 检查文件是否存在，如果不存在则写入表头
    import os
    write_header = not os.path.exists(file_name)
    with open(file_name, "a", encoding="utf-8", newline="") as f:
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
                sentiment_value = sentiment

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


"""
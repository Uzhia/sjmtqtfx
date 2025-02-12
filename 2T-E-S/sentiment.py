"""
import requests
import csv
import re
import time

# Ollama API 配置
API_URL = "http://localhost:11434/api/generate"
# MODEL_NAME = "deepseek-r1:14b"
MODEL_NAME = "deepseek-r1:1.5b"
INPUT_CSV = "comments.csv"  # 输入文件名（需提前准备好）
OUTPUT_CSV = "sentiment_results.csv"  # 输出文件名


def get_sentiment(comment):
    # 调用 Ollama API 获取情感值，并忽略 <think></think> 内容
    prompt = f'''你是一个情感分析工具。请将微博评论转化为 [0,1] 的立场值，规则：
- 负面[0,0.33)：表达愤怒/失望，如“太差了”
- 中立[0.33,0.66)：无倾向或混合评价，如“一般般”
- 积极[0.66,1]：明显赞扬，如“非常好”
直接返回数值（2位小数），格式：数值。

评论：“{comment}”'''

    try:
        response = requests.post(
            API_URL,
            json={
                "model": MODEL_NAME,
                "prompt": prompt,
                "stream": False
            }
        )
        response.raise_for_status()

        # 使用正则表达式提取数值（忽略 <think></think> 内容）
        result = re.search(r"(\d\.\d{2})(?:\s|$)", response.json()["response"])
        return float(result.group()) if result else None

    except Exception as e:
        print(f"处理失败: {comment} | 错误: {str(e)}")
        return None


# 处理评论并保存结果
with open(INPUT_CSV, "r", encoding="utf-8") as infile, \
        open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    writer.writerow(["评论内容", "情感值"])  # 写入表头

    for row in reader:
        comment = row[0].strip()  # 假设评论在每行第一列
        if not comment:
            continue

        # 获取情感值
        sentiment = get_sentiment(comment)

        # 写入结果（保留原始评论）
        writer.writerow([comment, sentiment])

        # 添加延迟防止请求过载
        time.sleep(0.5)

print("处理完成！结果已保存至", OUTPUT_CSV)
"""

import pymysql
import csv
import requests
import re
import time

# Ollama API 配置
API_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "deepseek-r1:1.5b"
OUTPUT_CSV = "sentiment_results_with_id.csv"  # 输出文件名

# 数据库连接信息
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '6',
    'database': 'weibo',
    'charset': 'utf8mb4'
}


def get_sentiment(comment):
    """调用 Ollama API 获取情感值，并忽略 <think></think> 内容"""
    prompt = f'''你是一个情感分析工具。请将微博评论转化为 [0,1] 的立场值，规则：
- 负面[0,0.33)：表达愤怒/失望，如“太差了”
- 中立[0.33,0.66)：无倾向或混合评价，如“一般般”
- 积极[0.66,1]：明显赞扬，如“非常好”
直接返回数值（2位小数），格式：数值。

评论：“{comment}”'''

    try:
        response = requests.post(
            API_URL,
            json={
                "model": MODEL_NAME,
                "prompt": prompt,
                "stream": False
            }
        )
        response.raise_for_status()

        # 使用正则表达式提取数值（忽略 <think></think> 内容）
        result = re.search(r"(\d\.\d{2})(?:\s|$)", response.json()["response"])
        return float(result.group()) if result else None

    except Exception as e:
        print(f"处理失败: {comment} | 错误: {str(e)}")
        return None


# 连接到数据库并获取评论数据
def get_comments_with_id_from_db():
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    try:
        # 假设评论存储在表 lg_cup_comments0210 的 'text' 字段中，并且需要获取 'id' 字段
        cursor.execute("SELECT id, text FROM lg_cup_comments0210 limit 10")
        results = cursor.fetchall()
        return [(row[0], row[1]) for row in results]
    except Exception as e:
        print(f"数据库查询失败: {str(e)}")
        return []
    finally:
        cursor.close()
        conn.close()


# 处理评论并保存结果
def process_comments():
    comments_with_id = get_comments_with_id_from_db()

    if not comments_with_id:
        print("未从数据库中获取到任何评论！")
        return

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["id", "评论内容", "情感值"])  # 写入表头

        for comment_data in comments_with_id:
            comment_id = comment_data[0]
            comment = comment_data[1].strip()
            if not comment:
                continue

            # 获取情感值
            sentiment = get_sentiment(comment)

            # 写入结果（保留原始评论和id）
            writer.writerow([comment_id, comment, sentiment])

            # 添加延迟防止请求过载
            time.sleep(0.5)

    print("处理完成！结果已保存至", OUTPUT_CSV)


if __name__ == "__main__":
    process_comments()
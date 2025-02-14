# encoding=utf-8

import pymysql
import requests
import json
import re

# 数据库配置
db_config = {
    "host": "localhost",  # 数据库地址
    "user": "root",  # 数据库用户名
    "password": "6",  # 数据库密码
    "database": "weibo",  # 数据库名称
    "charset": "utf8mb4",  # 字符集
    "port": 3306  # 端口号
}

# API 配置
api_url = "https://api.gpts.vin/v1/chat/completions"  # 替换为实际可用的API地址
api_key = "sk-"  # 替换为你的API密钥

# 清理文本的正则表达式
patterns = [
    (r"<(?!\/)a.*?>|<(\/)a.*?>|<(?!\/)a.*?>.*<(\/)a.*?>|<br />", ""),  # 去除a标签和换行符
    (r"<span.*?>(.*?)<\/span>", r"\1"),  # 保留span标签内容
    (r"#[^#]+#", ""),  # 去除话题标签
    (r"(\/\/@.*?:.*)|(\/\/@.*?：.*)|(@.*?:)|(@.*?：)", ""),  # 去除@引用
    (r"(\ud83c[\udf00-\udfff])|(\ud83d[\udc00-\ude4f\ude80-\udeff])|[\u2600-\u2B55]", ""),  # 去除emoji和符号
    (r"@[^\s]+[^\s]*的微博视频", "")  # 去除@...微博视频
]


# 连接数据库
def connect_db(config):
    try:
        conn = pymysql.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            port=config["port"],
            charset=config["charset"]
        )
        return conn
    except pymysql.MySQLError as e:
        print(f"数据库连接失败：{e}")
        return None


# 从数据库中读取数据
def fetch_data_from_db(conn, table_name):
    try:
        with conn.cursor() as cursor:
            sql = f"SELECT id, text FROM {table_name} WHERE text IS NOT NULL LIMIT 5"
            cursor.execute(sql)
            return cursor.fetchall()
    except pymysql.MySQLError as e:
        print(f"从数据库读取数据失败：{e}")
        return []


# 使用re清理文本
def clean_text(text):
    for pattern, repl in patterns:
        text = re.sub(pattern, repl, text, flags=re.UNICODE)
    return text.strip()


# 调用API获取JSON数据
def call_api(text):
    try:
        data = {
            "model": "gpt-3.5-turbo",
            "messages": [
                {"role": "system",
                 "content": "你是一个专业的事件信息抽取助手，擅长从文本中提取结构化的事件信息，其中event_type、event_subject、event_location、event_time有则输出，事件信息尽量简洁。请从用户提供的内容中提取关键信息，并以JSON格式返回结果"},
                {"role": "user", "content": text}
            ]
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        response = requests.post(api_url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API请求失败：{e}")
        return None


# 将结果写入txt文件
def write_to_txt(file_path, data_list):
    with open(file_path, "w", encoding="utf-8") as f:
        for item in data_list:
            f.write(f"{item['id']}\n")  # 写入id
            f.write(json.dumps(item['json_data'], ensure_ascii=False, indent=4))  # 写入JSON数据
            f.write("\n")  # 换行


# 主函数
def main():
    # 连接数据库
    conn = connect_db(db_config)
    if not conn:
        return

    # 从数据库中读取数据
    data_list = fetch_data_from_db(conn, "weibo0212")

    # 处理数据并调用API
    results = []
    for item in data_list:
        id, text = item
        cleaned_text = clean_text(text)
        api_response = call_api(cleaned_text)
        if api_response:
            json_data = api_response.get("choices", [{}])[0].get("message", {}).get("content", "")
            try:
                # 尝试解析为JSON
                parsed_json = json.loads(json_data)
                results.append({"id": id, "json_data": parsed_json})
            except json.JSONDecodeError:
                # 如果解析失败，直接保留原始响应
                results.append({"id": id, "json_data": json_data})
        else:
            # API返回为空，直接添加id和None
            results.append({"id": id, "json_data": None})

    # 写入txt文件
    write_to_txt("1_event_result.txt", results)

    # 关闭数据库连接
    conn.close()


if __name__ == "__main__":
    main()
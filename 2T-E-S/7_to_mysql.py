import csv
import pymysql

# 数据库连接配置
db_config = {
    'host': 'localhost',  # 数据库地址
    'user': 'root',      # 数据库用户名
    'password': '6',  # 数据库密码
    'database': 'weibo'  # 数据库名称
}

# CSV文件路径
csv_file_path = '7_merge_sentiment.csv'  # 确保文件名与实际文件名一致

# 连接到MySQL数据库
connection = pymysql.connect(**db_config)
cursor = connection.cursor()

# 读取CSV文件并插入数据
try:
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            comment_id = row['comment_id']
            sentiment = row['sentiment']
            # 构造SQL语句
            sql = "INSERT INTO comment_sentiment0212 (comment_id, sentiment) VALUES (%s, %s)"
            cursor.execute(sql, (comment_id, sentiment))
        connection.commit()
    print("数据导入成功！")
except Exception as e:
    print(f"导入数据时发生错误：{e}")
    connection.rollback()
finally:
    cursor.close()
    connection.close()
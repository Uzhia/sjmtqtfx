import csv
import pymysql

# 数据库连接信息
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '6',
    'database': 'weibo',
    'charset': 'utf8mb4'
}

# 连接到MySQL数据库
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

# 打开CSV文件
with open('D:/Download/test.csv', 'r', encoding='utf-8-sig') as csvfile:
    csvreader = csv.DictReader(csvfile)

    # 遍历CSV文件中的每一行
    for row in csvreader:
        # 插入数据到数据库
        cursor.execute(
            "INSERT INTO lg_cup_comments (id, weibo_id, user_id, text, comment_user_id, comment_user_name) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (row['id'], row['weibo_id'], row['user_id'], row['text'], row['comment_user_id'], row['comment_user_name'])
        )

# 提交事务
conn.commit()

# 关闭数据库连接
cursor.close()
conn.close()
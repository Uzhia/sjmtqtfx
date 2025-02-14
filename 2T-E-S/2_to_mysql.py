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
csv_file_path = '2_events_info.csv'

# 连接到MySQL数据库
connection = pymysql.connect(**db_config)
cursor = connection.cursor()

# 读取CSV文件并插入数据
try:
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            id = row['id']
            event_info = row['info']
            # 构造SQL语句
            sql = "INSERT INTO event0212 (id, event_info) VALUES (%s, %s)"
            cursor.execute(sql, (id, event_info))
        connection.commit()
    print("数据导入成功！")
except Exception as e:
    print(f"导入数据时发生错误：{e}")
    connection.rollback()
finally:
    cursor.close()
    connection.close()
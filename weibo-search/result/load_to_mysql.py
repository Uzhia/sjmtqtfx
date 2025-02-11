import pandas as pd
import pymysql
from pymysql.err import IntegrityError, DataError, InternalError

# 读取CSV文件
df = pd.read_csv('D:/Download/test_filled.csv')

# 连接MySQL数据库
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='6',
    database='weibo',
    charset='utf8mb4'
)
cursor = conn.cursor()

# 定义插入数据的SQL语句
insert_sql = """
INSERT INTO lg_cup (
    id, bid, user_id, screen_name, text, article_url, topics, at_users,
    reposts_count, comments_count, attitudes_count, created_at, source,
    video_url, retweet_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# 逐行插入数据
for index, row in df.iterrows():
    try:
        # 处理空值
        row = row.fillna({
            'user_id': None,
            'screen_name': None,
            'text': None,
            'article_url': None,
            'topics': None,
            'at_users': None,
            'video_url': None,
            'retweet_id': None,
            'location': None,
            'created_at': None,
            'source': None,
            'attitudes_count': 0,
            'comments_count': 0,
            'reposts_count': 0
        })

        # 转换数据类型，处理nan值
        row['attitudes_count'] = int(row['点赞数']) if pd.notnull(row['点赞数']) else 0
        row['comments_count'] = int(row['评论数']) if pd.notnull(row['评论数']) else 0
        row['reposts_count'] = int(row['转发数']) if pd.notnull(row['转发数']) else 0

        # 插入数据
        cursor.execute(insert_sql, (
            row['id'], row['bid'], row['user_id'], row['用户昵称'], row['微博正文'], row['头条文章url'],
            row['话题'], row['艾特用户'], row['转发数'], row['评论数'], row['点赞数'],
            row['发布时间'], row['发布工具'], row['微博视频url'], row['retweet_id']
        ))
        conn.commit()
    except (IntegrityError, DataError, InternalError) as e:
        print(f"Error on row {index}: {e}")
        conn.rollback()

# 关闭连接
cursor.close()

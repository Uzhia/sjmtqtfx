import scrapy
import pymysql
import json
import random
from weibo_comments.items import WeiboCommentsItem as CommentItem


def parse_cookie_string(cookie_str):
    cookies = {}
    for cookie in cookie_str.split(';'):
        if '=' in cookie:
            key, value = cookie.strip().split('=', 1)
            cookies[key] = value.strip()
    return cookies


class WeiboCommentsSpider(scrapy.Spider):
    name = 'weibo_comments'
    allowed_domains = ['weibo.com']
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Referer': 'https://weibo.com/',
    }
    long_cookie = ''
    cookies = parse_cookie_string(long_cookie)

    # 新增配置
    custom_settings = {
        'CONCURRENT_REQUESTS': 2,  # 降低并发量
        'DOWNLOAD_DELAY': random.randint(3, 8),  # 随机延迟
        'RETRY_TIMES': 5,  # 增加重试次数
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
        }
    }

    def start_requests(self):
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='6',
            database='weibo',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        table_name = "lg_cup"
        comments_table_name = "lg_cup_comments0210"

        try:
            with connection.cursor() as cursor:
                sql = f"""
                SELECT `id`, `user_id`, `comments_count` 
                FROM `{table_name}`
                WHERE `comments_count` > 0 
                AND NOT EXISTS (
                    SELECT 1 
                    FROM `{comments_table_name}` 
                    WHERE `{comments_table_name}`.`weibo_id` = `{table_name}`.`id`
                    GROUP BY weibo_id
                    HAVING COUNT(*) >= `{table_name}`.`comments_count` * 0.9
                );
                """
                cursor.execute(sql)
                results = cursor.fetchall()

                for row in results:
                    weibo_id = row['id']
                    user_id = row['user_id']
                    url = f'https://weibo.com/ajax/statuses/buildComments?flow=0&is_reload=1&id={weibo_id}&is_show_bulletin=3&is_mix=0&count=10&uid={user_id}&fetch_level=0'
                    yield scrapy.Request(
                        url=url,
                        headers=self.headers,
                        cookies=self.cookies,
                        callback=self.parse_comments,
                        meta={
                            'weibo_id': weibo_id,
                            'user_id': user_id,
                            'retry_times': 0,
                            'comment_count': row['comments_count']
                        }
                    )
        finally:
            connection.close()

    def parse_comments(self, response):
        meta = response.meta
        try:
            data = response.json()
            comments = data.get('data', [])

            # 立即提交item
            for comment_data in comments:
                text_raw = comment_data.get('text_raw')
                if text_raw:
                    user_info = comment_data.get('user', {})
                    yield CommentItem(
                        id=comment_data.get('id'),
                        weibo_id=meta['weibo_id'],
                        user_id=meta['user_id'],
                        text=text_raw,
                        comment_user_id=user_info.get('id'),
                        comment_user_name=user_info.get('screen_name')
                    )

            max_id = data.get('max_id', 0)
            if max_id and max_id > 0:
                # 动态调整请求参数
                next_url = f'https://weibo.com/ajax/statuses/buildComments?flow=0&is_reload=1&id={meta["weibo_id"]}&is_show_bulletin=3&is_mix=0&max_id={max_id}&count=20&uid={meta["user_id"]}'
                meta.update({
                    'max_id': max_id,
                    'retry_times': 0
                })
                yield scrapy.Request(
                    url=next_url,
                    headers=self.headers,
                    cookies=self.cookies,
                    callback=self.parse_comments,
                    meta=meta
                )
            else:
                self.logger.info(f"Reached last page for weibo {meta['weibo_id']}")

        except json.JSONDecodeError:
            self.logger.warning(f"JSONDecodeError occurred for weibo {meta['weibo_id']}")
            if meta.get('retry_times', 0) < 3:
                meta['retry_times'] += 1
                self.logger.info(f"Retrying weibo {meta['weibo_id']}, retry times: {meta['retry_times']}")
                yield scrapy.Request(
                    url=response.url,
                    headers=self.headers,
                    cookies=self.cookies,
                    callback=self.parse_comments,
                    meta=meta,
                    dont_filter=True
                )
            else:
                self.logger.error(f"Max retries exceeded for weibo {meta['weibo_id']}")

        except Exception as e:
            self.logger.error(f"Unexpected error processing weibo {meta['weibo_id']}: {str(e)}")
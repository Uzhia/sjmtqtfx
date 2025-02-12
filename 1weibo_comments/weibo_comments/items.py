# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class WeiboCommentsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id = scrapy.Field() # 这条评论的id
    weibo_id = scrapy.Field() # 这条评论所属的微博的id
    user_id = scrapy.Field() # 这条评论所属的微博，是用户id发出的
    text = scrapy.Field() # 这条评论的内容
    comment_user_id = scrapy.Field() # 这条评论的用户id
    comment_user_name = scrapy.Field() # 这条评论的用户名

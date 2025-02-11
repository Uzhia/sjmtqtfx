# -*- coding: utf-8 -*-
import os
import re
import sys
from datetime import datetime, timedelta
from urllib.parse import unquote

import requests
import scrapy

import weibo.utils.util as util
from scrapy.exceptions import CloseSpider
from scrapy.utils.project import get_project_settings
from weibo.items import WeiboItem


class SearchSpider(scrapy.Spider):
    # 爬虫的名称为 'search'
    name = 'search'
    # 允许爬取的域名是 'weibo.com'
    allowed_domains = ['weibo.com']
    # 获取项目的配置设置
    settings = get_project_settings()
    # 获取关键词列表
    keyword_list = settings.get('KEYWORD_LIST')
    # 如果关键词列表不是列表类型，则处理为列表
    if not isinstance(keyword_list, list):
        # 如果关键词列表的路径不是绝对路径，则转换为绝对路径
        if not os.path.isabs(keyword_list):
            keyword_list = os.getcwd() + os.sep + keyword_list
        # 如果关键词列表文件不存在，则退出程序并提示错误信息
        if not os.path.isfile(keyword_list):
            sys.exit('不存在%s文件' % keyword_list)
        # 从文件中读取关键词列表
        keyword_list = util.get_keyword_list(keyword_list)

    # 遍历关键词列表，对带#的热搜词进行编码
    for i, keyword in enumerate(keyword_list):
        if len(keyword) > 2 and keyword[0] == '#' and keyword[-1] == '#':
            keyword_list[i] = '%23' + keyword[1:-1] + '%23'
    # 获取微博类型
    weibo_type = util.convert_weibo_type(settings.get('WEIBO_TYPE'))
    # 获取包含类型
    contain_type = util.convert_contain_type(settings.get('CONTAIN_TYPE'))
    # 获取地区信息
    regions = util.get_regions(settings.get('REGION'))
    # 基础URL
    base_url = 'https://s.weibo.com'
    # 获取开始日期，默认为当前日期
    start_date = settings.get('START_DATE',
                              datetime.now().strftime('%Y-%m-%d'))
    # 获取结束日期，默认为当前日期
    end_date = settings.get('END_DATE', datetime.now().strftime('%Y-%m-%d'))
    # 如果开始日期晚于结束日期，则退出程序并提示错误信息
    if util.str_to_time(start_date) > util.str_to_time(end_date):
        sys.exit('settings.py配置错误，START_DATE值应早于或等于END_DATE值，请重新配置settings.py')
    # 获取进一步阈值，默认为46
    further_threshold = settings.get('FURTHER_THRESHOLD', 46)
    # 初始化数据库错误标志
    mongo_error = False
    pymongo_error = False
    mysql_error = False
    pymysql_error = False

    def start_requests(self):
        # 将开始日期字符串转换为 datetime 对象
        start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
        # 将结束日期字符串转换为 datetime 对象，并加1天，以包含结束日期当天的数据
        end_date = datetime.strptime(self.end_date,
                                     '%Y-%m-%d') + timedelta(days=1)
        # 构造开始时间字符串，格式为 'YYYY-MM-DD-0'
        start_str = start_date.strftime('%Y-%m-%d') + '-0'
        end_str = end_date.strftime('%Y-%m-%d') + '-0'
        # 遍历关键词列表
        for keyword in self.keyword_list:
            # 如果没有设置地区或地区为全部
            if not self.settings.get('REGION') or '全部' in self.settings.get(
                    'REGION'):
                # 构造基础URL，包含关键词
                base_url = 'https://s.weibo.com/weibo?q=%s' % keyword
                # 完整的URL，包含微博类型、包含类型和时间范围 
                url = base_url + self.weibo_type
                url += self.contain_type
                url += '&timescope=custom:{}:{}'.format(start_str, end_str)
                # 生成请求，回调函数为 self.parse，传递基础URL和关键词作为元数据
                yield scrapy.Request(
                                    # url示例：
                                    # https://s.weibo.com/weibo?q=%E5%B0%B9%E9%94%A1%E6%82%A6&scope=ori&suball=1&timescope=custom%3A2025-01-01-0%3A2025-01-11-0
                                    url=url,
                                     callback=self.parse,
                                     # 对url发送request后，使用parse进行页面解析。
                                     meta={
                                         'base_url': base_url,
                                         'keyword': keyword
                                     })
            else:
                for region in self.regions.values():
                    base_url = (
                        'https://s.weibo.com/weibo?q={}&region=custom:{}:1000'
                    ).format(keyword, region['code'])
                    url = base_url + self.weibo_type
                    url += self.contain_type
                    url += '&timescope=custom:{}:{}'.format(start_str, end_str)
                    # 获取一个省的搜索结果
                    yield scrapy.Request(url=url,
                                         callback=self.parse,
                                         meta={
                                             'base_url': base_url,
                                             'keyword': keyword,
                                             'province': region
                                         })

    def check_environment(self):
        """判断配置要求的软件是否已安装"""
        # 检查是否缺少pymongo库
        if self.pymongo_error:
            # 提示用户安装pymongo库
            print('系统中可能没有安装pymongo库，请先运行 pip install pymongo ，再运行程序')
            # 抛出 CloseSpider 异常，终止爬虫运行
            raise CloseSpider()
        # 检查是否缺少MongoDB数据库或未启动
        if self.mongo_error:
            # 提示用户安装或启动MongoDB
            print('系统中可能没有安装或启动MongoDB数据库，请先根据系统环境安装或启动MongoDB，再运行程序')
            # 抛出 CloseSpider 异常，终止爬虫运行
            raise CloseSpider()
        # 检查是否缺少pymysql库
        if self.pymysql_error:
            # 提示用户安装pymysql库
            print('系统中可能没有安装pymysql库，请先运行 pip install pymysql ，再运行程序')
            # 抛出 CloseSpider 异常，终止爬虫运行
            raise CloseSpider()
        # 检查是否缺少MySQL数据库或配置错误
        if self.mysql_error:
            # 提示用户安装或配置MySQL数据库
            print('系统中可能没有安装或正确配置MySQL数据库，请先根据系统环境安装或配置MySQL，再运行程序')
            # 抛出 CloseSpider 异常，终止爬虫运行
            raise CloseSpider()

    # 对url发送request后，使用parse进行页面解析。
    def parse(self, response):
        """
        解析对keyword的搜索结果页面的函数。
        
        该函数根据响应内容判断是否需要进一步抓取，或者是否需要按天进行抓取。
        如果搜索结果为空，则打印提示信息。如果搜索结果少于进一步抓取的阈值，
        则解析当前页面的微博信息，并尝试获取下一页。如果搜索结果多于阈值，
        则按天进行抓取，以避免单次抓取过多数据。
        
        参数:
        - response: Scrapy的响应对象，包含页面内容和其他元数据。
        
        返回:
        - 生成器，根据执行路径不同，可能返回微博信息或下一页的请求。
        """
        # 从响应中获取基础URL、关键词和省份信息
        # base_url示例：https://s.weibo.com/weibo?q=%E5%B0%B9%E9%94%A1%E6%82%A6
        base_url = response.meta.get('base_url')
        # keyword 示例： %E5%B0%B9%E9%94%A1%E6%82%A6
        keyword = response.meta.get('keyword')
        province = response.meta.get('province')
        
        # 检查当前页面是否显示“没有结果”
        is_empty = response.xpath(
            '//div[@class="card card-no-result s-pt20b40"]')
        
        # 计算搜索结果的页数
        page_count = len(response.xpath('//ul[@class="s-scroll"]/li'))
        
        # 如果当前页面搜索结果为空
        if is_empty:
            print('当前页面搜索结果为空')
        elif page_count < self.further_threshold:
            # 如果搜索结果页数少于进一步抓取的阈值
            # 解析当前页面的微博信息
            for weibo in self.parse_weibo(response):
                self.check_environment()
                yield weibo
            
            # 尝试获取下一页的URL
            next_url = response.xpath(
                '//a[@class="next"]/@href').extract_first()
            if next_url:
                next_url = self.base_url + next_url
                # 发起下一页的请求
                yield scrapy.Request(url=next_url,
                                    callback=self.parse_page,
                                    meta={'keyword': keyword})
        else:
            # 如果搜索结果页数多于阈值，按天进行抓取
            start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
            end_date = datetime.strptime(self.end_date, '%Y-%m-%d')
            
            # 遍历每一天，生成每天的搜索请求
            while start_date <= end_date:
                # e.g. 2025-01-11
                start_str = start_date.strftime('%Y-%m-%d') + '-0'
                # 设置时间间隔为1天
                start_date = start_date + timedelta(days=1)
                end_str = start_date.strftime('%Y-%m-%d') + '-0'
                url = base_url + self.weibo_type
                url += self.contain_type
                url += '&timescope=custom:{}:{}&page=1'.format(
                    start_str, end_str)
                # 获取一天的搜索结果,
                yield scrapy.Request(
                                    # url示例：
                                    # https://s.weibo.com/weibo?q=%E5%B0%B9%E9%94%A1%E6%82%A6&scope=ori&suball=1&timescope=custom:2025-01-10-0:2025-01-11-0&page=1
                                    url=url,
                                    callback=self.parse_by_day,
                                    meta={
                                        'base_url': base_url,
                                        'keyword': keyword,
                                        'province': province,
                                        'date': start_str[:-2]
                                    })

    def parse_by_day(self, response):
        """以天为单位筛选"""
        # 获取基础URL、关键词、省份等元数据
        # base_url示例：https://s.weibo.com/weibo?q=%E5%B0%B9%E9%94%A1%E6%82%A6
        base_url = response.meta.get('base_url')
        # keyword 示例： %E5%B0%B9%E9%94%A1%E6%82%A6
        keyword = response.meta.get('keyword')
        province = response.meta.get('province')
        # 检查页面是否为空
        is_empty = response.xpath(
            '//div[@class="card card-no-result s-pt20b40"]')
        # 获取日期和页面数量
        # date 示例 2025-01-11
        date = response.meta.get('date')
        page_count = len(response.xpath('//ul[@class="s-scroll"]/li'))
        # 根据页面内容进行不同操作
        if is_empty:
            print('当前页面搜索结果为空')
        elif page_count < self.further_threshold:
            # 解析当前页面
            for weibo in self.parse_weibo(response):
                self.check_environment()
                yield weibo
            # 获取下一页URL并请求
            next_url = response.xpath(
                '//a[@class="next"]/@href').extract_first()
            if next_url:
                next_url = self.base_url + next_url
                yield scrapy.Request(url=next_url,
                                    callback=self.parse_page,
                                    meta={'keyword': keyword})
        else:
            # 按小时分割日期范围并请求
            start_date_str = date + '-0'
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d-%H')
            for i in range(1, 25):
                start_str = start_date.strftime('%Y-%m-%d-X%H').replace(
                    'X0', 'X').replace('X', '')
                start_date = start_date + timedelta(hours=1)
                end_str = start_date.strftime('%Y-%m-%d-X%H').replace(
                    'X0', 'X').replace('X', '')
                url = base_url + self.weibo_type
                url += self.contain_type
                url += '&timescope=custom:{}:{}&page=1'.format(
                    start_str, end_str)
                # 获取一小时的搜索结果
                yield scrapy.Request(url=url,
                                    callback=self.parse_by_hour_province
                                    if province else self.parse_by_hour,
                                    meta={
                                        'base_url': base_url,
                                        'keyword': keyword,
                                        'province': province,
                                        'start_time': start_str,
                                        'end_time': end_str
                                    })

    def parse_by_hour(self, response):
        """以小时为单位筛选"""
        # 提取响应中的关键字
        keyword = response.meta.get('keyword')
        # 检查搜索结果是否为空
        is_empty = response.xpath(
            '//div[@class="card card-no-result s-pt20b40"]')
        # 提取响应中的开始时间和结束时间
        start_time = response.meta.get('start_time')
        end_time = response.meta.get('end_time')
        # 计算搜索结果的页数
        page_count = len(response.xpath('//ul[@class="s-scroll"]/li'))
        if is_empty:
            print('当前页面搜索结果为空')
        elif page_count < self.further_threshold:
            # 解析当前页面
            for weibo in self.parse_weibo(response):
                self.check_environment()
                yield weibo
            # 获取下一页的URL
            next_url = response.xpath(
                '//a[@class="next"]/@href').extract_first()
            if next_url:
                next_url = self.base_url + next_url
                yield scrapy.Request(url=next_url,
                                    callback=self.parse_page,
                                    meta={'keyword': keyword})
        else:
            # 遍历所有地区
            for region in self.regions.values():
                # 构造按地区筛选的请求URL
                url = ('https://s.weibo.com/weibo?q={}&region=custom:{}:1000'
                    ).format(keyword, region['code'])
                url += self.weibo_type
                url += self.contain_type
                url += '&timescope=custom:{}:{}&page=1'.format(
                    start_time, end_time)
                # 获取一小时一个省的搜索结果
                yield scrapy.Request(url=url,
                                    callback=self.parse_by_hour_province,
                                    meta={
                                        'keyword': keyword,
                                        'start_time': start_time,
                                        'end_time': end_time,
                                        'province': region
                                    })

    def parse_by_hour_province(self, response):
        """以小时和直辖市/省为单位筛选"""
        # 获取响应中的关键字、是否为空结果、开始和结束时间以及省份信息
        keyword = response.meta.get('keyword')
        is_empty = response.xpath(
            '//div[@class="card card-no-result s-pt20b40"]')
        start_time = response.meta.get('start_time')
        end_time = response.meta.get('end_time')
        province = response.meta.get('province')
        # 计算页面中的结果数量
        page_count = len(response.xpath('//ul[@class="s-scroll"]/li'))
        
        # 如果页面为空，则打印信息
        if is_empty:
            print('当前页面搜索结果为空')
        # 如果结果数量小于设定的阈值，则解析当前页面并尝试获取下一页
        elif page_count < self.further_threshold:
            # 解析当前页面
            for weibo in self.parse_weibo(response):
                self.check_environment()
                yield weibo
            # 获取下一页的URL
            next_url = response.xpath(
                '//a[@class="next"]/@href').extract_first()
            if next_url:
                next_url = self.base_url + next_url
                yield scrapy.Request(url=next_url,
                                    callback=self.parse_page,
                                    meta={'keyword': keyword})
        else:
            # 如果结果数量超过阈值，则按城市进一步细分搜索
            for city in province['city'].values():
                url = ('https://s.weibo.com/weibo?q={}&region=custom:{}:{}'
                    ).format(keyword, province['code'], city)
                url += self.weibo_type
                url += self.contain_type
                url += '&timescope=custom:{}:{}&page=1'.format(
                    start_time, end_time)
                # 获取一小时一个城市的搜索结果
                yield scrapy.Request(url=url,
                                    callback=self.parse_page,
                                    meta={
                                        'keyword': keyword,
                                        'start_time': start_time,
                                        'end_time': end_time,
                                        'province': province,
                                        'city': city
                                    })

        def parse_page(self, response):
            """解析一页搜索结果的信息

            Args:
                response: 包含搜索结果页面的响应对象
            """
            # 获取传递到该页面的关键词
            keyword = response.meta.get('keyword')
            # 检查页面是否显示“没有搜索结果”
            is_empty = response.xpath(
                '//div[@class="card card-no-result s-pt20b40"]')
            if is_empty:
                # 如果页面显示没有搜索结果，打印消息并返回
                print('当前页面搜索结果为空')
            else:
                # 如果页面有搜索结果，调用parse_weibo方法解析微博信息
                for weibo in self.parse_weibo(response):
                    # 在处理每个微博信息前，检查环境以确保操作的正确性
                    self.check_environment()
                    # 将解析后的微博信息yield出去，以便进一步处理或存储
                    yield weibo
                # 查找下一个页面的URL
                next_url = response.xpath(
                    '//a[@class="next"]/@href').extract_first()
                if next_url:
                    # 如果存在下一个页面的URL，将其与基础URL拼接
                    next_url = self.base_url + next_url
                    # 发起对下一个页面的请求，并将当前关键词作为元数据传递
                    yield scrapy.Request(url=next_url,
                                        callback=self.parse_page,
                                        meta={'keyword': keyword})

    def parse_page(self, response):
        """解析一页搜索结果的信息"""
        keyword = response.meta.get('keyword')
        is_empty = response.xpath(
            '//div[@class="card card-no-result s-pt20b40"]')
        if is_empty:
            print('当前页面搜索结果为空')
        else:
            for weibo in self.parse_weibo(response):
                self.check_environment()
                yield weibo
            next_url = response.xpath(
                '//a[@class="next"]/@href').extract_first()
            if next_url:
                next_url = self.base_url + next_url
                yield scrapy.Request(url=next_url,
                                     callback=self.parse_page,
                                     meta={'keyword': keyword})

    def get_ip(self, bid):
        """
        根据微博的bid获取用户的IP地址信息。

        Args:
            bid (str): 微博的唯一标识符。

        Returns:
            str: 用户的IP地址信息，如果获取失败或解析错误则返回空字符串。
        """
        # 构造请求URL以获取特定bid的微博数据
        url = f"https://weibo.com/ajax/statuses/show?id={bid}&locale=zh-CN"
        # 发送GET请求，使用预设的请求头
        response = requests.get(url, headers=self.settings.get('DEFAULT_REQUEST_HEADERS'))
        # 检查响应状态码，如果不为200则返回空字符串
        if response.status_code != 200:
            return ""
        try:
            # 尝试将响应内容解析为JSON格式
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            # 如果解析失败，返回空字符串
            return ""
        # 从解析的数据中获取地区名称
        ip_str = data.get("region_name", "")
        if ip_str:
            # 如果地区名称存在，取其最后一个部分作为IP地址信息
            ip_str = ip_str.split()[-1]
        # 返回IP地址信息
        return ip_str

    def get_article_url(self, selector):
        """获取微博头条文章url

        Args:
            selector: 用于选取HTML元素的selector对象

        Returns:
            string: 文章的URL地址，如果没有找到则返回空字符串
        """
        # 初始化文章URL为空字符串
        article_url = ''
        # 提取并清理文本内容，去除不必要的字符和空白
        text = selector.xpath('string(.)').extract_first().replace(
            '\u200b', '').replace('\ue627', '').replace('\n',
                                                        '').replace(' ', '')
        # 检查文本是否以特定前缀开始，表明可能存在文章URL
        if text.startswith('发布了头条文章'):
            # 获取所有链接元素
            urls = selector.xpath('.//a')
            for url in urls:
                # 查找包含特定类的图标，这通常与文章链接相关
                if url.xpath(
                        'i[@class="wbicon"]/text()').extract_first() == 'O':
                    # 检查链接是否以特定前缀开始，以确认它是有效的文章URL
                    if url.xpath('@href').extract_first() and url.xpath(
                            '@href').extract_first().startswith('http://t.cn'):
                        # 一旦找到有效URL，就赋值给article_url并终止循环
                        article_url = url.xpath('@href').extract_first()
                        break
        # 返回最终找到的文章URL，如果没有找到则返回空字符串
        return article_url

    def get_location(self, selector):
        """获取微博发布位置
        
        本函数通过解析传入的selector中的<a>标签，并寻找含有特定类名的<i>标签来确定微博的发布位置。
        如果找到符合条件的<i>标签，则提取其父元素<a>中的文本内容作为发布位置。
        
        Args:
            selector: 包含微博信息的选择器对象，用于XPath选择。
            
        Returns:
            location: 字符串类型，表示微博的发布位置。如果没有找到发布位置，则返回空字符串。
        """
        # 获取所有<a>标签
        a_list = selector.xpath('.//a')
        location = ''
        for a in a_list:
            # 检查<a>标签内是否有类名为'wbicon'的<i>标签，并且其文本内容为'2'
            if a.xpath('./i[@class="wbicon"]') and a.xpath(
                    './i[@class="wbicon"]/text()').extract_first() == '2':
                # 提取并保存发布位置信息
                location = a.xpath('string(.)').extract_first()[1:]
                break
        return location

    def get_at_users(self, selector):
        """获取微博中@的用户昵称

        Args:
            selector: 用于选取微博内容中的<a>标签的XPath选择器

        Returns:
            at_users: 以字符串形式返回微博中@的所有用户昵称，昵称之间用逗号分隔
        """
        # 获取所有子<a>标签
        a_list = selector.xpath('.//a')
        at_users = ''
        at_list = []
        for a in a_list:
            # 检查href属性长度和文本内容，以确定是否为@用户的链接
            if len(unquote(a.xpath('@href').extract_first())) > 14 and len(
                    a.xpath('string(.)').extract_first()) > 1:
                # 确保href属性中的用户ID与显示的用户名一致，以验证为有效的@用户
                if unquote(a.xpath('@href').extract_first())[14:] == a.xpath(
                        'string(.)').extract_first()[1:]:
                    at_user = a.xpath('string(.)').extract_first()[1:]
                    # 避免重复添加相同的用户昵称
                    if at_user not in at_list:
                        at_list.append(at_user)
        # 将列表中的用户昵称合并为逗号分隔的字符串
        if at_list:
            at_users = ','.join(at_list)
        return at_users

    def get_topics(self, selector):
        """获取参与的微博话题

        Args:
            selector: 一个selector对象，用于XPath选择，提取微博中的话题链接

        Returns:
            topics: 一个字符串，包含所有提取到的话题，话题之间用逗号分隔
        """
        # 提取所有<a>标签
        a_list = selector.xpath('.//a')
        topics = ''
        topic_list = []
        for a in a_list:
            # 提取<a>标签中的文本内容
            text = a.xpath('string(.)').extract_first()
            # 检查文本长度，并确认以#开头和结尾，标识为话题
            if len(text) > 2 and text[0] == '#' and text[-1] == '#':
                # 去除重复的话题并添加到话题列表
                if text[1:-1] not in topic_list:
                    topic_list.append(text[1:-1])
        # 如果话题列表不为空，则将它们连接成一个字符串
        if topic_list:
            topics = ','.join(topic_list)
        # 返回话题字符串
        return topics

    # 解析单个页面的所有微博内容
    def parse_weibo(self, response):
        """解析网页中的微博信息"""
        # 获取传递给该函数的关键词参数
        keyword = response.meta.get('keyword')
        
        # 遍历单个页面中所有的微博！
        for sel in response.xpath("//div[@class='card-wrap']"):
            # sel是一个单独的微博内容
            # 定位到微博内容的信息部分
            info = sel.xpath(
                "div[@class='card']/div[@class='card-feed']/div[@class='content']/div[@class='info']"
            )
            
            # 如果信息部分存在，则进行数据提取
            if info:
                weibo = WeiboItem()
                
                # 提取微博的ID
                weibo['id'] = sel.xpath('@mid').extract_first()
                
                # 提取微博的bid（备用ID）
                bid = sel.xpath(
                    './/div[@class="from"]/a[1]/@href').extract_first(
                ).split('/')[-1].split('?')[0]
                weibo['bid'] = bid
                
                # 提取用户的ID
                weibo['user_id'] = info[0].xpath(
                    'div[2]/a/@href').extract_first().split('?')[0].split(
                    '/')[-1]
                
                # 提取用户的昵称
                weibo['screen_name'] = info[0].xpath(
                    'div[2]/a/@nick-name').extract_first()
                
                # 定位到微博文本内容的选择器
                txt_sel = sel.xpath('.//p[@class="txt"]')[0]
                
                # 定位到转发的微博内容选择器
                retweet_sel = sel.xpath('.//div[@class="card-comment"]')
                retweet_txt_sel = ''
                
                # 如果存在转发内容，定位到转发内容的文本选择器
                if retweet_sel and retweet_sel[0].xpath('.//p[@class="txt"]'):
                    retweet_txt_sel = retweet_sel[0].xpath(
                        './/p[@class="txt"]')[0]
                
                # 判断是否为长微博，并进行相应的文本选择
                content_full = sel.xpath(
                    './/p[@node-type="feed_list_content_full"]')
                is_long_weibo = False
                is_long_retweet = False
                
                # 根据页面结构处理长微博和长转发内容
                if content_full:
                    if not retweet_sel:
                        txt_sel = content_full[0]
                        is_long_weibo = True
                    elif len(content_full) == 2:
                        txt_sel = content_full[0]
                        retweet_txt_sel = content_full[1]
                        is_long_weibo = True
                        is_long_retweet = True
                    elif retweet_sel[0].xpath(
                            './/p[@node-type="feed_list_content_full"]'):
                        retweet_txt_sel = retweet_sel[0].xpath(
                            './/p[@node-type="feed_list_content_full"]')[0]
                        is_long_retweet = True
                    else:
                        txt_sel = content_full[0]
                        is_long_weibo = True
                
                # 提取并处理微博文本内容
                weibo['text'] = txt_sel.xpath(
                    'string(.)').extract_first().replace('\u200b', '').replace(
                    '\ue627', '')
                
                # 提取文章链接
                weibo['article_url'] = self.get_article_url(txt_sel)
                
                # 提取位置信息
                weibo['location'] = self.get_location(txt_sel)
                
                # 如果存在位置信息，从文本中移除
                if weibo['location']:
                    weibo['text'] = weibo['text'].replace(
                        '2' + weibo['location'], '')
                
                # 进一步处理文本内容
                weibo['text'] = weibo['text'][2:].replace(' ', '')
                
                # 如果是长微博，移除结尾的多余部分
                if is_long_weibo:
                    weibo['text'] = weibo['text'][:-4]
                
                # 提取@用户信息
                weibo['at_users'] = self.get_at_users(txt_sel)
                
                # 提取话题信息
                weibo['topics'] = self.get_topics(txt_sel)
                
                # 提取并处理转发数
                reposts_count = sel.xpath(
                    './/a[@action-type="feed_list_forward"]/text()').extract()
                reposts_count = "".join(reposts_count)
                try:
                    reposts_count = re.findall(r'\d+.*', reposts_count)
                except TypeError:
                    print(
                        "无法解析转发按钮，可能是 1) 网页布局有改动 2) cookie无效或已过期。\n"
                        "请在 https://github.com/dataabc/weibo-search 查看文档，以解决问题，"
                    )
                    raise CloseSpider()
                weibo['reposts_count'] = reposts_count[
                    0] if reposts_count else '0'
                
                # 提取并处理评论数
                comments_count = sel.xpath(
                    './/a[@action-type="feed_list_comment"]/text()'
                ).extract_first()
                comments_count = re.findall(r'\d+.*', comments_count)
                weibo['comments_count'] = comments_count[
                    0] if comments_count else '0'
                attitudes_count = sel.xpath(
                    './/a[@action-type="feed_list_like"]/button/span[2]/text()').extract_first()
                attitudes_count = re.findall(r'\d+.*', attitudes_count)
                weibo['attitudes_count'] = attitudes_count[
                    0] if attitudes_count else '0'
                
                # 提取并处理创建时间
                created_at = sel.xpath(
                    './/div[@class="from"]/a[1]/text()').extract_first(
                ).replace(' ', '').replace('\n', '').split('前')[0]
                weibo['created_at'] = util.standardize_date(created_at)
                
                # 提取来源信息
                source = sel.xpath('.//div[@class="from"]/a[2]/text()'
                                ).extract_first()
                weibo['source'] = source if source else ''
                
                # 提取图片链接
                pics = ''
                is_exist_pic = sel.xpath(
                    './/div[@class="media media-piclist"]')
                if is_exist_pic:
                    pics = is_exist_pic[0].xpath('ul[1]/li/img/@src').extract()
                    pics = [pic[8:] for pic in pics]
                    pics = [
                        re.sub(r'/.*?/', '/large/', pic, 1) for pic in pics
                    ]
                    pics = ['https://' + pic for pic in pics]
                
                # 提取视频链接
                video_url = ''
                is_exist_video = sel.xpath(
                    './/div[@class="thumbnail"]//video-player').extract_first()
                if is_exist_video:
                    video_url = re.findall(r'src:\'(.*?)\'', is_exist_video)[0]
                    video_url = video_url.replace('&amp;', '&')
                    video_url = 'http:' + video_url
                
                # 根据是否存在转发内容，决定是否填充图片和视频链接
                if not retweet_sel:
                    weibo['pics'] = pics
                    weibo['video_url'] = video_url
                else:
                    weibo['pics'] = ''
                    weibo['video_url'] = ''
                
                # 处理转发微博的ID
                weibo['retweet_id'] = ''
                
                # 如果存在转发内容，提取转发内容的相关信息
                if retweet_sel and retweet_sel[0].xpath(
                        './/div[@node-type="feed_list_forwardContent"]/a[1]'):
                    retweet = WeiboItem()
                    retweet['id'] = retweet_sel[0].xpath(
                        './/a[@action-type="feed_list_like"]/@action-data'
                    ).extract_first()[4:]
                    retweet['bid'] = retweet_sel[0].xpath(
                        './/p[@class="from"]/a/@href').extract_first().split(
                        '/')[-1].split('?')[0]
                    info = retweet_sel[0].xpath(
                        './/div[@node-type="feed_list_forwardContent"]/a[1]'
                    )[0]
                    retweet['user_id'] = info.xpath(
                        '@href').extract_first().split('/')[-1]
                    retweet['screen_name'] = info.xpath(
                        '@nick-name').extract_first()
                    retweet['text'] = retweet_txt_sel.xpath(
                        'string(.)').extract_first().replace('\u200b',
                                                            '').replace(
                        '\ue627', '')
                    retweet['article_url'] = self.get_article_url(
                        retweet_txt_sel)
                    retweet['location'] = self.get_location(retweet_txt_sel)
                    if retweet['location']:
                        retweet['text'] = retweet['text'].replace(
                            '2' + retweet['location'], '')
                    retweet['text'] = retweet['text'][2:].replace(' ', '')
                    if is_long_retweet:
                        retweet['text'] = retweet['text'][:-4]
                    retweet['at_users'] = self.get_at_users(retweet_txt_sel)
                    retweet['topics'] = self.get_topics(retweet_txt_sel)
                    reposts_count = retweet_sel[0].xpath(
                        './/ul[@class="act s-fr"]/li[1]/a[1]/text()'
                    ).extract_first()
                    reposts_count = re.findall(r'\d+.*', reposts_count)
                    retweet['reposts_count'] = reposts_count[
                        0] if reposts_count else '0'
                    comments_count = retweet_sel[0].xpath(
                        './/ul[@class="act s-fr"]/li[2]/a[1]/text()'
                    ).extract_first()
                    comments_count = re.findall(r'\d+.*', comments_count)
                    retweet['comments_count'] = comments_count[
                        0] if comments_count else '0'
                    attitudes_count = retweet_sel[0].xpath(
                        './/a[@class="woo-box-flex woo-box-alignCenter woo-box-justifyCenter"]//span[@class="woo-like-count"]/text()'
                    ).extract_first()
                    attitudes_count = re.findall(r'\d+.*', attitudes_count)
                    retweet['attitudes_count'] = attitudes_count[
                        0] if attitudes_count else '0'
                    created_at = retweet_sel[0].xpath(
                        './/p[@class="from"]/a[1]/text()').extract_first(
                    ).replace(' ', '').replace('\n', '').split('前')[0]
                    retweet['created_at'] = util.standardize_date(created_at)
                    source = retweet_sel[0].xpath(
                        './/p[@class="from"]/a[2]/text()').extract_first()
                    retweet['source'] = source if source else ''
                    retweet['pics'] = pics
                    retweet['video_url'] = video_url
                    retweet['retweet_id'] = ''
                    yield {'weibo': retweet, 'keyword': keyword}
                    weibo['retweet_id'] = retweet['id']
                
                # 提取IP信息
                weibo["ip"] = self.get_ip(bid)
                
                # 提取用户认证信息
                avator = sel.xpath(
                    "div[@class='card']/div[@class='card-feed']/div[@class='avator']"
                )
                # 检查微博用户头像部分是否存在，以提取用户认证信息
                if avator:
                    # 提取用户认证图标ID
                    user_auth = avator.xpath('.//svg/@id').extract_first()
                    # 打印用户认证图标ID（调试用途）
                    print(user_auth)
                    # 根据不同的认证图标ID设置用户的认证类型
                    if user_auth == 'woo_svg_vblue':
                        weibo['user_authentication'] = '蓝V'
                    elif user_auth == 'woo_svg_vyellow':
                        weibo['user_authentication'] = '黄V'
                    elif user_auth == 'woo_svg_vorange':
                        weibo['user_authentication'] = '红V'
                    elif user_auth == 'woo_svg_vgold':
                        weibo['user_authentication'] = '金V'
                    else:
                        weibo['user_authentication'] = '普通用户'
                # 打印解析后的微博数据（调试用途）
                print(weibo)
                # 生成并返回包含微博数据和关键词的字典
                yield {'weibo': weibo, 'keyword': keyword}
import hashlib
import logging
import re
import time
from enum import unique, Enum
from threading import Thread, current_thread
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from pymongo import MongoClient, errors
from requests import ConnectionError as req_ConnectionError, HTTPError, get
from redis import Redis, ConnectionError as red_ConnectionError, BusyLoadingError, DataError

"""
该爬虫对格言网网页中数据进行整站URL提取进行爬取数据。
AUTH：TTC
DATE：2018年6月2日 09:19:39
"""

HOST_URL = 'https://www.geyanw.com/'  # 主站地址
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0'  # 用户代理
PROXIES = ['122.114.31.177:808',
           '61.135.217.7:80',
           '36.33.25.70:808',
           '222.85.50.177:808',
           '59.60.175.10:30086',
           '61.178.238.122:63000',
           '218.28.58.150:53281']  # 代理地址
REDIS_GEYANW = 'geyanw_task'  # redisURL待访问列表
REDIS_VISITED = 'visited_urls'  # redis URL访问过的集合


def http_retry(fn):
    """
    HTTP 链接装饰器
    :param fn: 函数
    :return: 执行装饰后 的函数
    """
    def wrapper(*args, **kwargs):
        # 尝试五次
        for i in range(5):
            try:
                # 无异常正常返回函数
                return fn(*args, **kwargs)
            except (req_ConnectionError, HTTPError) as e:
                # 连接出错休眠3秒继续尝试
                logging.error(e)
                print(f'{current_thread().getName()},Requests操作出现错误3秒后尝试重新操作， 第{i+1}次尝试')
                time.sleep(3)

    return wrapper


def db_retry_conn(fn):
    """
    数据库链接重新尝试
    :param fn: 函数
    :return: 执行装饰后的函数
    """
    def wrapper(*args, **kwargs):
        # 尝试3次
        for i in range(3):
            try:
                # 无异常正常返回函数
                return fn(*args, **kwargs)
            except (red_ConnectionError, BusyLoadingError, DataError) as e:
                # 抓取Redis的连接错误
                logging.error(e)
                print(f'{current_thread().getName()},Redis操作出现错误3秒后尝试重新操作， 第{i+1}次尝试')
                time.sleep(3)
            except (errors.AutoReconnect, errors.DuplicateKeyError) as e:
                # 抓取Mongo的连接错误
                logging.error(e)
                print(f'{current_thread().getName()}, Mongo操作出现错误3秒后尝试重新操作， 第{i+1}次尝试')
                time.sleep(3)

    return wrapper


class DBController(object):
    """数据库控制"""
    def __init__(self):
        self._redis = Redis(host='118.24.88.26', port=8464, password='qwer')  # redis对象
        self._mongo = MongoClient(host='118.24.88.26', port=27017)  # mongo连接对象
        self._motto = self._mongo.geyanw.motto  # 格言数据集合

    @db_retry_conn
    def redis_is_exist_list(self, list_name):
        """
        判断redis中是否已经存在redis list对象
        :param list_name: list的名字
        :return: Boolean
        """
        return self._redis.exists(list_name)

    @db_retry_conn
    def redis_push_url_to_lib(self, list_name, url):
        """
        将URL添加到待爬取redis list中
        :param list_name: list的名字
        :param url: URL字符串
        :return: int
        """
        return self._redis.rpush(list_name, url)

    @db_retry_conn
    def redis_push_url_to_set(self, set_name, url):
        """
        将爬取过的URL添加到redis集合中
        :param set_name: 集合名称
        :param url: URL字符串
        :return: int
        """
        return self._redis.sadd(set_name, url)

    @db_retry_conn
    def redis_pop_url_from_lib(self, list_name):
        """
        从redis list中取数据
        :param list_name: list的名称
        :return: string
        """
        return self._redis.lpop(list_name)

    @db_retry_conn
    def redis_get_list_len(self, list_name):
        """
        判断list中数据长度
        :param list_name: list名称
        :return: int
        """
        return self._redis.llen(list_name)

    @db_retry_conn
    def redis_is_member_in_set(self, set_name, url):
        """
        判断URL是否已经存在于集合中
        :param set_name: 集合名称
        :param url: URL字符串
        :return: Boolean
        """
        return self._redis.sismember(set_name, url)

    @db_retry_conn
    def mongo_motto_insert_one(self, data):
        """
        向mongo数据库中插入一条数据
        :param data: 封装好的数据字典
        :return: int
        """
        return self._motto.insert_one(data)

    @db_retry_conn
    def mongo_motto_is_exist(self, data):
        """
        判断数据在数据库中是否已经存在
        :param data: 封装好的数据字典
        :return: Boolean
        """
        return True if self._motto.find_one({'_id': data['_id']}) else False


@unique
class SpiderStatus(Enum):
    """枚举对象"""
    IDLE = 0  # 闲置
    WORKING = 1  # 繁忙


class Spider(object):
    """爬虫对象"""

    def __init__(self):
        """初始化方法"""
        self._status = SpiderStatus.IDLE  # 初始状态为闲置
        self._charsets = ('GB2312', 'UTF-8', 'GBK')  # 默认的一些字符集元组
        self._domain = 'www.geyanw.com'  # 站点主站netloc

    def set_status_idle(self):
        """
        改变状态为闲置
        :return: None
        """
        self._status = SpiderStatus.IDLE

    def set_status_working(self):
        """
        改变状态为繁忙
        :return: None
        """
        self._status = SpiderStatus.WORKING

    @property
    def status(self):
        """
        获取当前爬虫状态
        :return: int
        """
        return self._status

    @http_retry
    def fetch(self, current_url):
        """
        获取传递进来的URL的赫塔米勒页面数据
        :param current_url: URL字符串
        :return: HTML字符串数据
        """
        headers = {'user-agent': USER_AGENT}  # HTTP头，设置代理，标识身份
        resp = get(current_url, headers=headers)  # request请求页面
        # 成功就返回对应的字符，失败返回None
        return self.decode_page(resp.content) if resp.status_code == 200 else None

    def decode_page(self, page):
        """
        对respaonse进行解码
        :param page:
        :return:
        """
        for charset in self._charsets:
            try:
                html_page = page.decode(charset)  # 进行解码
                return html_page
            except UnicodeDecodeError as e:  # 抓取解码错误的异常
                logging.error(e)
        return None  # 解码失败返回None

    def parse(self, html_page, current_url):
        """
        解析网页中的URL数据
        :param html_page: 网页html数据
        :param current_url:  当前网页的URL
        :return: None
        """
        # soup文档对象
        soup = BeautifulSoup(html_page, 'lxml')
        # 遍历所有的a标签
        for a_tag in soup.body.select('a[href]'):
            a_url = a_tag.attrs['href']  # 获取href属性
            parser = urlparse(a_url)  # 解析URL
            scheme = parser.scheme or 'http'  # 为空就赋值为http
            netloc = parser.netloc or self._domain  # 为空就赋值为主站地址
            if scheme != 'javascript' and netloc == self._domain:  # 排除javascript的链接
                path = parser.path  # 路径
                if re.match(r'^list_[\d?]_[\d?]\.html$', a_url):  # 匹配翻页链接
                    new_url = current_url[:str(current_url).rfind('/') + 1] + a_url  # 拼接为完整的URL连接
                    print(new_url)
                else:
                    new_url = f'{scheme}://{netloc}{path}'  # 将URL拼接完整
                if not db.redis_is_member_in_set(REDIS_VISITED, new_url):  # 如果连接以及被访问过就不进行操作了
                    db.redis_push_url_to_lib(REDIS_GEYANW, new_url)  # 将新的连接添加到redis待访问缓存中

    def extract(self, html_page):
        """
        HTML页面的解析
        :param html_page:  HTML页面字符
        :return: None
        """
        # html文档树对象
        soup = BeautifulSoup(html_page, 'lxml')
        # 判断是否是数据详情页面，是就进行数据的解析，不是就就跳过
        if soup.select_one('div[class="content"]'):
            # 当格言的类型
            motto_type = soup.select('div[class="position"] a')[1].text
            # 当前格言的标题
            title = soup.select_one('div [class="title"] h2').text
            # 循环遍历类容中的p标签
            for itemp_p in soup.select('div[class="content"] p'):
                motto = itemp_p.text  # 获取text文本
                hasher = hasher_proto.copy()  # 复制一个加密对象
                hasher.update(motto.encode('utf-8'))  # 对格言生成摘要
                motto_id = hasher.hexdigest()  # 获取16进制的摘要
                # 不为空进行数据封装
                if motto != '\xa0' and motto is not None:
                    try:
                        motto = motto.split('、')  # 进行清洗
                        if len(motto) >= 2:
                            motto = motto[1]
                        else:
                            return
                    except IndexError as e:
                        logging.error(e)
                        pass
                    else:
                        # 封装成字典
                        data = {'_id': motto_id,
                                'type': motto_type,
                                'title': title,
                                'motto': motto}
                        print(data)
                        self.store(data)  # 进行存储

    @db_retry_conn
    def store(self, data):
        """
        格言封装对象的存储
        :param data: 封装好的对象
        :return: None
        """
        # 在数据库中是否存在相同的摘要
        if not db.mongo_motto_is_exist(data):
            db.mongo_motto_insert_one(data)  # 添加数据


class SpiderThread(Thread):
    """爬虫线程"""
    def __init__(self, name):
        super().__init__(name=name)  # 初始化父类，和线程名称
        self._spider = Spider()  # 初始化爬虫对象

    def run(self):
        """
        重写线程开始运行的回调对象
        :return: None
        """
        # 已知进行爬取
        while True:
            # 从缓存中获取数据
            current_url = db.redis_pop_url_from_lib(REDIS_GEYANW)
            # 如果没获取到就一直获取
            while not current_url:
                current_url = db.redis_pop_url_from_lib(REDIS_GEYANW)
            # 缓存集合中是否已经获取过
            if not db.redis_is_member_in_set(REDIS_VISITED, current_url):
                self._spider.set_status_working()  # 修改爬虫状态为繁忙
                current_url = current_url.decode('utf-8')  # 对获取到的URL进行UTF-8解码
                html_page = self._spider.fetch(current_url)  # 获取当前URL连接的页面
                if html_page not in [None, '']:  # html页面是否为空
                    self._spider.extract(html_page)  # 不为空进行解析数据
                    self._spider.parse(html_page, current_url)  # 进行解析URL数据
                db.redis_push_url_to_set(REDIS_VISITED, current_url)  # 将获取过的URL添加到已访问集合中
                self._spider.set_status_idle()  # 修改爬虫状态为闲置


db = DBController()  # 全局DB控制对象
hasher_proto = hashlib.sha1()  # 全局数据加密对象


def is_any_spider_alive(spider_threads):
    """
    判断爬虫线程是否忙绿
    :param spider_threads:
    :return:
    """
    return any([spider_thread.spider.status == SpiderStatus.WORKING
                for spider_thread in spider_threads])


def main():
    # 如果不存在url列表，放入主站网址
    if not db.redis_is_exist_list(REDIS_GEYANW):
        db.redis_push_url_to_lib(REDIS_GEYANW, HOST_URL)
    # 创建10个爬虫线程对象
    spider_threads = [SpiderThread('Thread-%d' % i) for i in range(10)]
    # 开启10个爬虫
    for spider_thread in spider_threads:
        spider_thread.start()
    # 判断所有爬虫是否都已经完成了工作
    while db.redis_get_list_len != 0 or is_any_spider_alive(spider_threads):
        pass


if __name__ == '__main__':
    main()

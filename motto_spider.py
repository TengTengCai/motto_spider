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

HOST_URL = 'https://www.geyanw.com/'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0'
PROXIES = ['122.114.31.177:808',
           '61.135.217.7:80',
           '36.33.25.70:808',
           '222.85.50.177:808',
           '59.60.175.10:30086',
           '61.178.238.122:63000',
           '218.28.58.150:53281']
REDIS_GEYANW = 'geyanw_task'
REDIS_VISITED = 'visited_urls'


def http_retry(fn):
    def wrapper(*args, **kwargs):
        for i in range(5):
            try:
                return fn(*args, **kwargs)
            except (req_ConnectionError, HTTPError) as e:
                logging.error(e)
                print(f'{current_thread().getName()},Requests操作出现错误3秒后尝试重新操作， 第{i+1}次尝试')
                time.sleep(3)

    return wrapper


def db_retry_conn(fn):
    def wrapper(*args, **kwargs):
        for i in range(3):
            try:
                return fn(*args, **kwargs)
            except (red_ConnectionError, BusyLoadingError, DataError) as e:
                logging.error(e)
                print(f'{current_thread().getName()},Redis操作出现错误3秒后尝试重新操作， 第{i+1}次尝试')
                time.sleep(3)
            except (errors.AutoReconnect, errors.DuplicateKeyError) as e:
                logging.error(e)
                print(f'{current_thread().getName()}, Mongo操作出现错误3秒后尝试重新操作， 第{i+1}次尝试')
                time.sleep(3)

    return wrapper


class DBController(object):
    def __init__(self):
        self._redis = Redis(host='', port=, password='')
        self._mongo = MongoClient(host='', port=)
        self._motto = self._mongo.geyanw.motto

    @db_retry_conn
    def redis_is_exist_list(self, list_name):
        return self._redis.exists(list_name)

    @db_retry_conn
    def redis_push_url_to_lib(self, list_name, url):
        return self._redis.rpush(list_name, url)

    @db_retry_conn
    def redis_push_url_to_set(self, set_name, url):
        return self._redis.sadd(set_name, url)

    @db_retry_conn
    def redis_pop_url_from_lib(self, list_name):
        return self._redis.lpop(list_name)

    @db_retry_conn
    def redis_get_list_len(self, list_name):
        return self._redis.llen(list_name)

    @db_retry_conn
    def redis_is_member_in_set(self, set_name, url):
        return self._redis.sismember(set_name, url)

    @db_retry_conn
    def mongo_motto_insert_one(self, data):
        return self._motto.insert_one(data)

    @db_retry_conn
    def mongo_motto_is_exist(self, data):
        return True if self._motto.find_one({'_id': data['_id']}) else False


@unique
class SpiderStatus(Enum):
    IDLE = 0
    WORKING = 1


class Spider(object):

    def __init__(self):
        self._status = SpiderStatus.IDLE
        self._charsets = ('GB2312', 'UTF-8', 'GBK')
        self._domain = 'www.geyanw.com'

    def set_status_idle(self):
        self._status = SpiderStatus.IDLE

    def set_status_working(self):
        self._status = SpiderStatus.WORKING

    @property
    def status(self):
        return self._status

    @http_retry
    def fetch(self, current_url):
        headers = {'user-agent': USER_AGENT}
        resp = get(current_url, headers=headers)
        return self.decode_page(resp.content) if resp.status_code == 200 else None

    def decode_page(self, page):
        """
        对respaonse进行解码
        :param page:
        :return:
        """
        for charset in self._charsets:
            try:
                html_page = page.decode(charset)
                return html_page
            except UnicodeDecodeError as e:
                logging.error(e)
        return None

    def parse(self, html_page, current_url):
        soup = BeautifulSoup(html_page, 'lxml')
        for a_tag in soup.body.select('a[href]'):
            a_url = a_tag.attrs['href']
            parser = urlparse(a_url)
            scheme = parser.scheme or 'http'
            netloc = parser.netloc or self._domain
            if scheme != 'javascript' and netloc == self._domain:
                path = parser.path
                if re.match(r'^list_[\d?]_[\d?]\.html$', a_url):
                    new_url = current_url[:str(current_url).rfind('/') + 1] + a_url
                    print(new_url)
                else:
                    new_url = f'{scheme}://{netloc}{path}'
                if not db.redis_is_member_in_set(REDIS_VISITED, new_url):
                    db.redis_push_url_to_lib(REDIS_GEYANW, new_url)

    def extract(self, html_page):
        soup = BeautifulSoup(html_page, 'lxml')
        if soup.select_one('div[class="content"]'):
            motto_type = soup.select('div[class="position"] a')[1].text
            title = soup.select_one('div [class="title"] h2').text
            for itemp_p in soup.select('div[class="content"] p'):
                motto = itemp_p.text
                hasher = hasher_proto.copy()
                hasher.update(motto.encode('utf-8'))
                motto_id = hasher.hexdigest()
                if motto != '\xa0' and motto is not None:
                    try:
                        motto = motto.split('、')
                        if len(motto) >= 2:
                            motto = motto[1]
                        else:
                            return
                    except IndexError as e:
                        logging.error(e)
                        pass
                    else:
                        data = {'_id': motto_id,
                                'type': motto_type,
                                'title': title,
                                'motto': motto}
                        print(data)
                        self.store(data)

    @db_retry_conn
    def store(self, data):
        if not db.mongo_motto_is_exist(data):
            db.mongo_motto_insert_one(data)


class SpiderThread(Thread):
    def __init__(self, name):
        super().__init__(name=name)
        self._spider = Spider()

    def run(self):
        while True:
            current_url = db.redis_pop_url_from_lib(REDIS_GEYANW)
            while not current_url:
                current_url = db.redis_pop_url_from_lib(REDIS_GEYANW)
            if not db.redis_is_member_in_set(REDIS_VISITED, current_url):
                self._spider.set_status_working()
                current_url = current_url.decode('utf-8')
                html_page = self._spider.fetch(current_url)
                if html_page not in [None, '']:
                    self._spider.extract(html_page)
                    self._spider.parse(html_page, current_url)
                db.redis_push_url_to_set(REDIS_VISITED, current_url)
                self._spider.set_status_idle()


db = DBController()
hasher_proto = hashlib.sha1()


def is_any_spider_alive(spider_threads):
    """
    判断爬虫线程是否忙绿
    :param spider_threads:
    :return:
    """
    return any([spider_thread.spider.status == SpiderStatus.WORKING
                for spider_thread in spider_threads])


def main():
    if not db.redis_is_exist_list(REDIS_GEYANW):
        db.redis_push_url_to_lib(REDIS_GEYANW, HOST_URL)
    spider_threads = [SpiderThread('Thread-%d' % i) for i in range(10)]
    for spider_thread in spider_threads:
        spider_thread.start()

    while db.redis_get_list_len != 0 or is_any_spider_alive(spider_threads):
        pass


if __name__ == '__main__':
    main()

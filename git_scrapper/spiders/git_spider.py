import os.path
import sqlite3
import time

import scrapy
import pathlib
import logging

from git_scrapper.items import GitScrapperItem

BASE_DIR = pathlib.Path(__file__).parent.parent.parent.resolve()

logging.basicConfig(
    level=logging.DEBUG,
    filename=BASE_DIR / 'logs/scraped.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class GitSpider(scrapy.Spider):
    name = 'git_spider'
    allowed_domains = ['google.com']
    domain = 'https://github.com/'
    OUTPUT_DB_FILE_NAME = 'scraped.db'
    INPUT_FILE_NAME = 'urls.txt'
    OUTPUT_FOLDER_NAME = 'outputs'
    INPUT_FOLDER_NAME = 'inputs'

    @staticmethod
    def get_input_url():
        inputs_dir = BASE_DIR / GitSpider.INPUT_FOLDER_NAME
        file_name = GitSpider.INPUT_FILE_NAME
        with open(inputs_dir / file_name, 'r') as f:
            url = f.readline().strip()
            print(f'url reading....... as url: {url}')
        return url

    @staticmethod
    def get_next_scraping_url():
        scraped_db_file_path = BASE_DIR / GitSpider.OUTPUT_FOLDER_NAME / GitSpider.OUTPUT_DB_FILE_NAME
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        res = cursor.execute(f'''SELECT * FROM scraped ORDER BY ID DESC LIMIT 1;''')
        print(f'Last row is  {res}')
        cursor.close()
        connection.close()

        res = list(res)[0]
        repository_url = res[1]
        request_url = res[2]
        next_url = res[3]

        return next_url

    @staticmethod
    def is_db_file_empty():
        # file is 100% existing so just get count of rows
        scraped_db_file_path = BASE_DIR / GitSpider.OUTPUT_FOLDER_NAME / GitSpider.OUTPUT_DB_FILE_NAME
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        res = cursor.execute(f'''SELECT COUNT(*) FROM scraped;''')
        print(f'Count is {res}')
        cursor.close()
        connection.close()

        return res

    @staticmethod
    def create_database():
        scraped_db_file_path = BASE_DIR / GitSpider.OUTPUT_FOLDER_NAME / GitSpider.OUTPUT_DB_FILE_NAME
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS scraped(
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                repository_url TEXT,
                request_url TEXT,
                next_url TEXT
            );
            '''
        )
        connection.commit()
        cursor.close()
        connection.close()

    @staticmethod
    def get_start_url():
        outputs_dir = BASE_DIR / GitSpider.OUTPUT_FOLDER_NAME
        file_name = GitSpider.OUTPUT_DB_FILE_NAME

        if not os.path.exists(outputs_dir / file_name):
            GitSpider.create_database()
            url = GitSpider.get_input_url()
        else:
            if GitSpider.is_db_file_empty():
                url = GitSpider.get_input_url()
            else:
                url = GitSpider.get_next_scraping_url()

        return url

    @staticmethod
    def get_parser_name_from_url(url):
        domain = GitSpider.domain
        search_prefix = domain + 'search?'

        if url[:len(search_prefix)] == search_prefix:
            return 'search_result', url

        if url[:len(domain)] == domain:
            splited = url[len(domain):].split('/')
            if len(splited) == 1:
                return 'main', url + '?tab=repositories'
            elif len(splited) == 2:
                return 'repo', url

        message = f'Url: {url} for parsing is INVALID!!'
        logging.error(message)

        raise Exception('Url is not valid')

    @staticmethod
    def common_parser(response, css_path, callback):
        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url if next_url else None

        for suf in suffixes:
            url = GitSpider.domain + suf
            # save this link somewhere
            # yield GitSpider.save_link(url, page_url, next_url)
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = next_url
            yield items

        if next_url:
            url = GitSpider.domain + next_url.get()
            yield scrapy.Request(url=url, callback=callback)

    def start_requests(self):
        callbacks_list = {
            'main': self.main_parse,
            'search_result': self.search_result_parse,
            'repo': self.repo_parse
        }
        url = GitSpider.get_start_url()

        message = f'Start Url is \'{url}\''
        logging.info(message)

        print(url, '............................')
        if url:
            key, url = GitSpider.get_parser_name_from_url(url)
            callback = callbacks_list[key]
            yield scrapy.Request(url=url, callback=callback)

    def main_parse(self, response):
        css_path = '#user-repositories-list ul li h3 a::attr(href)'
        callback = self.main_parse
        # yield GitSpider.common_parser(response, css_path, callback)

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url.get() if next_url else None

        for suf in suffixes:
            url = GitSpider.domain + suf[1:]
            # save this link somewhere
            # yield GitSpider.save_link(url, page_url, next_url)
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = GitSpider.domain + next_url[1:]
            yield items
        print(next_url, 'eeeeeeeeeeeeeeeeeee')
        if next_url:
            url = GitSpider.domain + next_url[1:]
            print(f'next_page url {url}')
            yield scrapy.Request(url=url, callback=callback)

    def search_result_parse(self, response):
        css_path = '.repo-list-item div div div.text-normal>a::attr(href)'
        callback = self.search_result_parse
        # yield GitSpider.common_parser(response, css_path, callback)

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url.get() if next_url else None

        for suf in suffixes:
            url = GitSpider.domain + suf[1:]
            # save this link somewhere
            # yield GitSpider.save_link(url, page_url, next_url)
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = GitSpider.domain + next_url[1:]
            yield items

        if next_url:
            url = GitSpider.domain + next_url[1:]
            yield scrapy.Request(url=url, callback=callback)

    def repo_parse(self, response):
        url = response.request.url
        # save this link somewhere
        # yield GitSpider.save_link(url, url, None)

        items = GitScrapperItem()
        items['url'] = url + '.git'
        items['page_url'] = url
        items['next_url'] = None
        yield items

import os.path
import sqlite3

import scrapy
import pathlib

BASE_DIR = pathlib.Path(__file__).parent.parent.parent.resolve()


class GitSpider(scrapy.Spider):
    name = 'git_spider'
    allowed_domains = ['google.com']
    domain = 'https://github.com'
    OUTPUT_DB_FILE_NAME = 'scraped.db'
    LOG_FILE_NAME = 'scraped.log'
    INPUT_FILE_NAME = 'urls.txt'

    @staticmethod
    def get_input_url():
        inputs_dir = BASE_DIR / 'inputs'
        file_name = GitSpider.INPUT_FILE_NAME
        with open(inputs_dir / file_name, 'r') as f:
            url = f.readline().strip()
            print(f'url reading....... as url: {url}')
        return url

    @staticmethod
    def get_last_scraped_url():

        # get_last_scraped_url
        # increase page by 1
        pass

    @staticmethod
    def is_db_file_empty():
        # file is 100% existing so just get count of rows
        scraped_db_file_path = BASE_DIR / "outputs/scraped.db"
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        res = cursor.execute(f'''SELECT COUNT(*) FROM scraped;''')
        print(f'Count is {res}')
        cursor.close()
        connection.close()

        return res

    @staticmethod
    def create_database():
        scraped_db_file_path = BASE_DIR / "outputs/scraped.db"
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        cursor.execute(
            f'''CREATE TABLE IF NOT EXISTS scraped(repository_url TEXT, request_url TEXT, next_url TEXT);'''
        )
        connection.commit()
        cursor.close()
        connection.close()

    @staticmethod
    def get_start_url():
        outputs_dir = BASE_DIR / 'outputs'
        file_name = GitSpider.OUTPUT_DB_FILE_NAME

        if not os.path.exists(outputs_dir / file_name):
            # create db file
            GitSpider.create_database()
            url = GitSpider.get_input_url()
        else:
            if GitSpider.is_db_file_empty():
                url = GitSpider.get_input_url()
            else:
                url = GitSpider.get_last_scraped_url()

        return url

    @staticmethod
    def insert_into_database(repository_url, request_url, next_url):
        scraped_db_file_path = BASE_DIR / "outputs/scraped.db"
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        res = cursor.execute(f'''INSERT INTO scraped VALUES('{repository_url}', '{request_url}', '{next_url}');''')
        print(f'Count is {res}')
        cursor.close()
        connection.close()

    @staticmethod
    def insert_into_log(repository_url, request_url, next_url):
        pass

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
            GitSpider.save_link(url, page_url, next_url)

        if next_url:
            url = GitSpider.domain + next_url.get()
            yield scrapy.Request(url=url, callback=callback)

    @staticmethod
    def save_link(url, request_url, next_url):
        repository_url = url + '.git'
        GitSpider.insert_into_database(repository_url, request_url, next_url)
        GitSpider.insert_into_log(repository_url, request_url, next_url)

    def start_requests(self):
        callbacks_list = {
            'main': self.main_parse,
            'search_result': self.search_result_parse,
            'repo': self.repo_parse
        }
        url = GitSpider.get_start_url()
        key, url = GitSpider.get_parser_name_from_url(url)
        callback = callbacks_list[key]
        yield scrapy.Request(url=url, callback=callback)

    def main_parse(self, response):
        css_path = '#user-repositories-list ul li h3 a::attr(href)'
        callback = self.main_parse
        GitSpider.common_parser(response, css_path, callback)

    def search_result_parse(self, response):
        css_path = '.repo-list-item div div div.text-normal>a::attr(href)'
        callback = self.search_result_parse
        GitSpider.common_parser(response, css_path, callback)

    def repo_parse(self, response):
        url = response.request.url
        # save this link somewhere
        GitSpider.save_link(url, url, None)

import json
import os.path
import sqlite3
import time
from logging.handlers import RotatingFileHandler

import scrapy
import pathlib
import logging

from git_scrapper.items import GitScrapperItem
from scrapy.utils.log import configure_logging

from git_scrapper.pipelines import GitScrapperPipeline

BASE_DIR = pathlib.Path(__file__).parent.parent.parent.resolve()

LOG_ENABLED = False
# Disable default Scrapy log settings.
configure_logging(install_root_handler=False)
log_file = BASE_DIR / 'outputs/scraped.log'

root_logger = logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotating_file_log = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=1)
rotating_file_log.setLevel(logging.DEBUG)
rotating_file_log.setFormatter(formatter)
root_logger.addHandler(rotating_file_log)


class GitSpider(scrapy.Spider):
    name = 'git_spider'
    allowed_domains = ['google.com']
    domain = 'https://github.com/'
    OUTPUT_DB_FILE_NAME = 'scraped.db'
    INPUT_FILE_NAME = 'urls.txt'
    OUTPUT_FOLDER_NAME = 'outputs'
    INPUT_FOLDER_NAME = 'inputs'
    TOPIC_JSON_FILE_NAME = 'topic.json'

    @staticmethod
    def insert_topic_data(funct_name, current_page, next_page, topic_name=None):
        inputs_dir = BASE_DIR / GitSpider.OUTPUT_FOLDER_NAME / GitSpider.TOPIC_JSON_FILE_NAME

        # Format
        # dictionary = {
        #     'search_topics_result': {
        #         'current_page': '',
        #         'next_page': '',
        #     },
        #     'search_topics_specific': {
        #         'topic_name': {
        #             'current_page': '',
        #             'next_page': '',
        #         }
        #     }
        # }

        json_object = {}
        if os.path.exists(inputs_dir):
            with open(inputs_dir, 'r') as openfile:
                json_object = json.load(openfile)

        if topic_name:
            if json_object.get(funct_name, None) is None:
                json_object[funct_name] = dict()
            if json_object[funct_name].get(topic_name, None) is None:
                json_object[funct_name][topic_name] = {'count': 0}
            json_object[funct_name][topic_name]['current_page'] = current_page
            json_object[funct_name][topic_name]['next_page'] = next_page
            json_object[funct_name][topic_name]['count'] += 1
        else:
            if json_object.get(funct_name, None) is None:
                json_object[funct_name] = {
                    'count': 0,
                }
            json_object[funct_name]['current_page'] = current_page
            json_object[funct_name]['next_page'] = next_page
            json_object[funct_name]['count'] += 1

        json_object = json.dumps(json_object, indent=4)

        with open(inputs_dir, 'w') as file:
            file.write(json_object)

    @staticmethod
    def get_input_url():
        inputs_file_path = BASE_DIR / GitSpider.INPUT_FOLDER_NAME / GitSpider.INPUT_FILE_NAME

        with open(inputs_file_path, 'r') as f:
            url = f.readline().strip()
        return url

    @staticmethod
    def is_base_url_topic_search():
        urls_file_path = BASE_DIR / GitSpider.INPUT_FOLDER_NAME / GitSpider.INPUT_FILE_NAME

        with open(urls_file_path, 'r') as f:
            url = f.readline().strip()

        if 'type=topics' in url or 'type=Topics' in url:
            return True
        return False

    @staticmethod
    def get_next_topic_search_url():
        inputs_dir = BASE_DIR / GitSpider.OUTPUT_FOLDER_NAME / GitSpider.TOPIC_JSON_FILE_NAME

        with open(inputs_dir, 'r') as openfile:
            json_object = json.load(openfile)

        return json_object

    @staticmethod
    def get_next_scraping_url():
        if GitSpider.is_base_url_topic_search():
            return GitSpider.get_next_topic_search_url()

        scraped_db_file_path = BASE_DIR / GitSpider.OUTPUT_FOLDER_NAME / GitSpider.OUTPUT_DB_FILE_NAME
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        res = cursor.execute(f'''SELECT * FROM scraped ORDER BY ID DESC LIMIT 1;''')

        next_url = None
        for i in res:
            next_url = i[3]

        cursor.close()
        connection.close()
        return next_url if next_url != 'None' else None

    @staticmethod
    def is_db_file_empty():
        # file is 100% existing so just get count of rows
        scraped_db_file_path = BASE_DIR / GitSpider.OUTPUT_FOLDER_NAME / GitSpider.OUTPUT_DB_FILE_NAME
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        res = cursor.execute(f'''SELECT COUNT(*) FROM scraped;''')
        count = 0
        for i in res:
            count = i[0]
        cursor.close()
        connection.close()

        return count == 0

    @staticmethod
    def get_start_url():
        outputs_dir = BASE_DIR / GitSpider.OUTPUT_FOLDER_NAME
        file_name = GitSpider.OUTPUT_DB_FILE_NAME

        if not os.path.exists(outputs_dir / file_name):
            message = 'Database does\'t exists.'
            root_logger.debug(message)

            url = GitSpider.get_input_url()
        else:
            if GitSpider.is_db_file_empty():
                message = 'Database is empty.'
                root_logger.debug(message)

                url = GitSpider.get_input_url()
            else:
                url = GitSpider.get_next_scraping_url()
        return url

    @staticmethod
    def get_parser_name_from_url(url):
        domain = GitSpider.domain
        search_prefix = domain + 'search?'

        if url[:len(search_prefix)] == search_prefix:
            if 'type=issues' in url or 'type=Issues' in url:
                return 'search_issue_result', url
            elif 'type=discussions' in url or 'type=Discussions' in url:
                return 'search_discussion_result', url
            elif 'type=topics' in url or 'type=Topics' in url:
                return 'search_topics_result', url
            elif 'type=wikis' in url or 'type=Wikis' in url:
                return 'search_wikis_result', url
            elif 'type=commits' in url or 'type=Commits' in url:
                return 'search_commit_result', url
            return 'search_repositories_result', url

        if url[:len(domain)] == domain:
            splited = url[len(domain):].split('/')
            if len(splited) == 1:
                url = url if ('tab=repositories' in url) else url + '?tab=repositories'
                return 'main', url
            elif len(splited) == 2:
                if splited[0] == 'topics':
                    return 'search_topics_specific', url
                return 'repo', url

        message = f'Url: {url} is not Valid! Maybe you have scraped all repositories OR your input url is incorrect.'
        root_logger.warning(message)
        return None, None

    def start_requests(self):
        message = 'Started Scrapping.'
        root_logger.info(message)

        callbacks_list = {
            'main': self.main_parse,
            'repo': self.repo_parse,
            'search_repositories_result': self.search_repositories_result_parse,
            'search_issue_result': self.search_issue_result_parse,
            'search_discussion_result': self.search_discussion_result_parse,
            'search_wikis_result': self.search_wikis_result_parse,
            'search_topics_result': self.search_topics_result_parse,
            'search_topics_specific': self.search_topics_specific,
            'search_commit_result': self.search_commit_result_parse,
        }
        url = GitSpider.get_start_url()

        if isinstance(url, dict):
            message = 'Found \'Topic\' keyword in search url.'
            root_logger.debug(message)

            base_next_url = url['search_topics_result']['next_page']

            message = f'Start Url for Base Topic is \'{base_next_url}\''
            root_logger.info(message)

            if base_next_url:
                message = f'Starting URL For Topics is \'{base_next_url}\''
                root_logger.info(message)

                yield scrapy.Request(url=base_next_url, callback=self.search_topics_result_parse)

            for topic_name, value in url['search_topics_specific'].items():
                specific_topic_next_url = value['next_page']
                if specific_topic_next_url:
                    message = f'Start URL for Sub Topics [{topic_name}] is \'{specific_topic_next_url}\''
                    root_logger.info(message)
                    yield scrapy.Request(url=specific_topic_next_url, callback=self.search_topics_specific)
        else:
            message = f'Starting URL is \'{url}\''
            root_logger.info(message)

            if url:
                key, url = GitSpider.get_parser_name_from_url(url)

                message = f'Key for URL is {key}'
                root_logger.debug(message)

                callback = callbacks_list[key]
                yield scrapy.Request(url=url, callback=callback)

    def main_parse(self, response):
        css_path = '#user-repositories-list ul li h3 a::attr(href)'
        callback = self.main_parse

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url.get() if next_url else None

        for suf in suffixes:
            NUMBER_OF_LINKS_TO_SCRAP = os.environ.get('NUMBER_OF_LINKS_TO_SCRAP')
            if NUMBER_OF_LINKS_TO_SCRAP:
                if GitScrapperPipeline.count_data() >= int(NUMBER_OF_LINKS_TO_SCRAP):
                    return

            url = GitSpider.domain + suf[1:]
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = (GitSpider.domain + next_url[1:]) if next_url else next_url
            yield items

        if next_url:
            message = f'Found next page. URL: \'{next_url}\''
            root_logger.debug(message)

            url = GitSpider.domain + next_url[1:]
            yield scrapy.Request(url=url, callback=callback)

    def search_repositories_result_parse(self, response):
        css_path = '.repo-list-item div div div.text-normal>a::attr(href)'
        callback = self.search_repositories_result_parse

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url.get() if next_url else None

        for suf in suffixes:
            NUMBER_OF_LINKS_TO_SCRAP = os.environ.get('NUMBER_OF_LINKS_TO_SCRAP')
            if NUMBER_OF_LINKS_TO_SCRAP:
                if GitScrapperPipeline.count_data() >= int(NUMBER_OF_LINKS_TO_SCRAP):
                    return

            url = GitSpider.domain + suf[1:]
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = (GitSpider.domain + next_url[1:]) if next_url else next_url
            yield items

        if next_url:
            message = f'Found next page. URL: \'{next_url}\''
            root_logger.debug(message)

            url = GitSpider.domain + next_url[1:]
            yield scrapy.Request(url=url, callback=callback)

    def search_issue_result_parse(self, response):
        css_path = '.issue-list-item div div.text-normal>a::attr(href)'
        callback = self.search_issue_result_parse

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url.get() if next_url else None

        for suf in suffixes:
            NUMBER_OF_LINKS_TO_SCRAP = os.environ.get('NUMBER_OF_LINKS_TO_SCRAP')
            if NUMBER_OF_LINKS_TO_SCRAP:
                if GitScrapperPipeline.count_data() >= int(NUMBER_OF_LINKS_TO_SCRAP):
                    return

            url = GitSpider.domain + '/'.join(suf[1:].split('/')[:2])
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = (GitSpider.domain + next_url[1:]) if next_url else next_url
            yield items

        if next_url:
            message = f'Found next page. URL: \'{next_url}\''
            root_logger.debug(message)

            url = GitSpider.domain + next_url[1:]
            yield scrapy.Request(url=url, callback=callback)

    def search_discussion_result_parse(self, response):
        css_path = '.discussion-list-item div div.text-normal>a::attr(href)'
        callback = self.search_discussion_result_parse

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url.get() if next_url else None

        for suf in suffixes:
            NUMBER_OF_LINKS_TO_SCRAP = os.environ.get('NUMBER_OF_LINKS_TO_SCRAP')
            if NUMBER_OF_LINKS_TO_SCRAP:
                if GitScrapperPipeline.count_data() >= int(NUMBER_OF_LINKS_TO_SCRAP):
                    return

            url = GitSpider.domain + '/'.join(suf[1:].split('/')[:2])
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = (GitSpider.domain + next_url[1:]) if next_url else next_url
            yield items

        if next_url:
            message = f'Found next page. URL: \'{next_url}\''
            root_logger.debug(message)

            url = GitSpider.domain + next_url[1:]
            yield scrapy.Request(url=url, callback=callback)

    def search_commit_result_parse(self, response):
        css_path = '#commit_search_results>div>div>div:nth-child(1)>a::attr(href)'
        callback = self.search_commit_result_parse

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url.get() if next_url else None

        for suf in suffixes:
            NUMBER_OF_LINKS_TO_SCRAP = os.environ.get('NUMBER_OF_LINKS_TO_SCRAP')
            if NUMBER_OF_LINKS_TO_SCRAP:
                if GitScrapperPipeline.count_data() >= int(NUMBER_OF_LINKS_TO_SCRAP):
                    return

            url = GitSpider.domain + '/'.join(suf[1:].split('/')[:2])
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = (GitSpider.domain + next_url[1:]) if next_url else next_url
            yield items

        if next_url:
            message = f'Found next page. URL: \'{next_url}\''
            root_logger.debug(message)

            url = GitSpider.domain + next_url[1:]
            yield scrapy.Request(url=url, callback=callback)

    def search_wikis_result_parse(self, response):
        css_path = '.hx_hit-wiki div.text-normal>a::attr(href)'
        callback = self.search_wikis_result_parse

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url.get() if next_url else None

        for suf in suffixes:
            NUMBER_OF_LINKS_TO_SCRAP = os.environ.get('NUMBER_OF_LINKS_TO_SCRAP')
            if NUMBER_OF_LINKS_TO_SCRAP:
                if GitScrapperPipeline.count_data() >= int(NUMBER_OF_LINKS_TO_SCRAP):
                    return

            url = GitSpider.domain + '/'.join(suf[1:].split('/')[:2])
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = (GitSpider.domain + next_url[1:]) if next_url else next_url
            yield items

        if next_url:
            message = f'Found next page. URL: \'{next_url}\''
            root_logger.debug(message)

            url = GitSpider.domain + next_url[1:]
            yield scrapy.Request(url=url, callback=callback)

    def search_topics_result_parse(self, response):
        css_path = '.topic-list-item div div.text-normal>a::attr(href)'
        callback = self.search_topics_result_parse

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_url = response.css('.next_page::attr(href)')
        next_url = next_url.get() if next_url else None

        for suf in suffixes:
            url = GitSpider.domain + suf[1:]
            yield scrapy.Request(url=url, callback=self.search_topics_specific)

        GitSpider.insert_topic_data('search_topics_result', page_url, next_url)

        if next_url:
            message = f'Found next page. URL: \'{next_url}\''
            root_logger.debug(message)

            url = GitSpider.domain + next_url[1:]
            yield scrapy.Request(url=url, callback=callback)

    def search_topics_specific(self, response):
        css_path = 'article>div>div>div>h3>a:nth-of-type(2)::attr(href)'
        callback = self.search_topics_specific

        def get_next_url(current_page_url, page_lis):
            current_page_url = '?'.join(
                current_page_url.split('?')[:-1]
            ) if '?page' in current_page_url else current_page_url
            return current_page_url + f'?page={page_lis.get()}'

        page_url = response.request.url
        suffixes = response.css(css_path).extract()
        next_page_number = response.css('form.ajax-pagination-form > input::attr(value)')
        next_url = get_next_url(page_url, next_page_number) if next_page_number else None

        for suf in suffixes:
            NUMBER_OF_LINKS_TO_SCRAP = os.environ.get('NUMBER_OF_LINKS_TO_SCRAP')
            if NUMBER_OF_LINKS_TO_SCRAP:
                if GitScrapperPipeline.count_data() >= int(NUMBER_OF_LINKS_TO_SCRAP):
                    return

            url = GitSpider.domain + '/'.join(suf[1:].split('/')[:2])
            items = GitScrapperItem()
            items['url'] = url
            items['page_url'] = page_url
            items['next_url'] = next_url
            yield items

            topic_name = (page_url.split('?')[0]).split('/')[-1]
            GitSpider.insert_topic_data('search_topics_specific', page_url, next_url, topic_name=topic_name)

        if next_url:
            message = f'Found next page. URL: \'{next_url}\''
            root_logger.debug(message)

            yield scrapy.Request(url=next_url, callback=callback)

    def repo_parse(self, response):
        url = response.request.url
        items = GitScrapperItem()
        items['url'] = url + '.git'
        items['page_url'] = url
        items['next_url'] = None
        yield items

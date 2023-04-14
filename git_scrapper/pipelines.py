import logging
import os
import pathlib
import sqlite3
from logging.handlers import RotatingFileHandler

from scrapy.exceptions import CloseSpider
from scrapy.utils.log import configure_logging

BASE_DIR = pathlib.Path(__file__).parent.parent.resolve()

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


class GitScrapperPipeline:
    OUTPUT_DB_FILE_NAME = 'scraped.db'
    OUTPUT_FOLDER_NAME = 'outputs'

    def open_spider(self, spider):
        GitScrapperPipeline.create_database()

    def process_item(self, item, spider):
        url = item['url']
        page_url = item['page_url']
        next_url = item['next_url']
        GitScrapperPipeline.save_link(url, page_url, next_url)
        return item

    def close_spider(self, spider):
        count = GitScrapperPipeline.count_data()
        message = f'Total repositories is {count}'
        root_logger.info(message)

    @staticmethod
    def count_data():
        scraped_db_file_path = BASE_DIR / 'outputs/scraped.db'
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        res = cursor.execute(f'''SELECT COUNT(*) FROM scraped;''')
        count = 0
        for i in res:
            count = i[0]
        cursor.close()
        connection.close()

        return count

    @staticmethod
    def add_link_to_collected_links(url):
        collected_links_file_path = BASE_DIR / 'outputs/collected_links.txt'

        with open(collected_links_file_path, 'a') as file:
            file.write(f"{url}\n")

        message = f'Added {url} to collected_links.txt.'
        root_logger.debug(message)

    @staticmethod
    def create_database():
        scraped_db_file_path = BASE_DIR / GitScrapperPipeline.OUTPUT_FOLDER_NAME / GitScrapperPipeline.OUTPUT_DB_FILE_NAME
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        message = f'Database creating.'
        root_logger.debug(message)

        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS scraped(
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                repository_url TEXT,
                request_url TEXT,
                next_url TEXT
            );'''
        )

        message = f'Database Created.'
        root_logger.debug(message)

        connection.commit()
        cursor.close()
        connection.close()

    @staticmethod
    def insert_into_database(repository_url, request_url, next_url):
        scraped_db_file_path = BASE_DIR / GitScrapperPipeline.OUTPUT_FOLDER_NAME / GitScrapperPipeline.OUTPUT_DB_FILE_NAME
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        cursor.execute(
            f'''INSERT INTO scraped(repository_url, request_url, next_url) VALUES('{repository_url}', '{request_url}', '{next_url}');''')

        message = f'Inserting into database. Repository URL: {repository_url}, Request URL: {request_url}, Next URL: {next_url} '
        root_logger.debug(message)

        connection.commit()
        cursor.close()
        connection.close()

    @staticmethod
    def save_link(url, request_url, next_url):
        GitScrapperPipeline.add_link_to_collected_links(url)

        repository_url = url + '.git'

        message = f'Scrapped URL: \'{repository_url}\''
        root_logger.debug(message)

        GitScrapperPipeline.insert_into_database(repository_url, request_url, next_url)



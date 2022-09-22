import logging
import pathlib
import sqlite3

BASE_DIR = pathlib.Path(__file__).parent.parent.resolve()

logging.basicConfig(
    level=logging.DEBUG,
    filename=BASE_DIR / 'logs/scraped.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class GitScrapperPipeline:
    OUTPUT_DB_FILE_NAME = 'scraped.db'
    OUTPUT_FOLDER_NAME = 'outputs'
    def process_item(self, item, spider):
        url = item['url']
        page_url = item['page_url']
        next_url = item['next_url']
        print(url, page_url, next_url, 'ooooooooooooooooooooooooooooooooo')

        GitScrapperPipeline.save_link(url, page_url, next_url)

        return item

    @staticmethod
    def insert_into_database(repository_url, request_url, next_url):
        scraped_db_file_path = BASE_DIR / GitScrapperPipeline.OUTPUT_FOLDER_NAME / GitScrapperPipeline.OUTPUT_DB_FILE_NAME
        connection = sqlite3.connect(scraped_db_file_path)
        cursor = connection.cursor()

        cursor.execute(
            f'''INSERT INTO scraped(repository_url, request_url, next_url) VALUES('{repository_url}', '{request_url}', '{next_url}');''')

        message = 'Inserting into database with values as -> repository_url: {repository_url}, request_url: {' \
                  'request_url}, next_url: {next_url} '
        logging.debug(message)

        connection.commit()
        cursor.close()
        connection.close()
    @staticmethod
    def save_link(url, request_url, next_url):
        repository_url = url + '.git'
        GitScrapperPipeline.insert_into_database(repository_url, request_url, next_url)

        message = f'Scrapped \'{repository_url}\' with request url: \'{request_url}\''
        logging.info(message)

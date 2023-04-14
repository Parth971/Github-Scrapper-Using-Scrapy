import logging
import os
import pathlib
import sqlite3
import subprocess
from utils import get_all_data, get_downloaded_urls, get_collected_links
from logging.handlers import RotatingFileHandler

from scrapy.utils.log import configure_logging

BASE_DIR = pathlib.Path(__file__).parent.resolve()

LOG_ENABLED = False
# Disable default Scrapy log settings.
configure_logging(install_root_handler=False)
log_file = BASE_DIR / 'outputs/downloading.log'

root_logger = logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotating_file_log = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=1)
rotating_file_log.setLevel(logging.DEBUG)
rotating_file_log.setFormatter(formatter)
root_logger.addHandler(rotating_file_log)

class CloningProcess:
    def run(self):
        urls = CloningProcess.get_repo_urls()
        CloningProcess.start_download(urls)

    @classmethod
    def get_repo_urls(cls):
        return get_collected_links() - get_downloaded_urls()


    @classmethod
    def downloaded_link(cls, link):
        downloaded_link_file_path = BASE_DIR / 'outputs/downloaded_link.txt'
        with open(downloaded_link_file_path, 'a') as file:
            file.write(link + '\n')

    @classmethod
    def delete_repo(cls, downloading_folder_path):
        os.system(f'rmdir /s "{downloading_folder_path}"')
        root_logger.debug(f'{downloading_folder_path} Deleted Successfully.')

    @classmethod
    def start_download(cls, urls):

        if not urls:
            print('\n---------- No Repository URLs found to be downloaded, Maybe all repositories are already downloaded ----------\n')
            root_logger.debug('No urls found in database. Check database for more details.')
            return

        print(f'Found {len(urls)} urls to downloaded.')
        root_logger.info(f'Found {len(urls)} urls to download.')

        all_repos_successfully_downloaded = True
        unsuccessful_urls = []
        for url in urls:
            folder_name = url.split('/')[-2]
            sub_folder_name = url.split('/')[-1][:-4]
            downloading_folder_path = BASE_DIR / f'RepoDownloads/{folder_name}'

            if os.path.exists(downloading_folder_path / sub_folder_name):
                root_logger.debug(f'{url} already exists. May be half downloaded. Deleting it.')
                CloningProcess.delete_repo(downloading_folder_path/sub_folder_name)

            clone_cmd = f'''git clone --progress {url} "{downloading_folder_path / sub_folder_name}"'''

            proc = subprocess.Popen(clone_cmd, shell=True)
            while proc.poll() is None:
                pass

            root_logger.debug(f'Return code for {url} is {proc.returncode}')

            if proc.returncode == 0:
                CloningProcess.downloaded_link(url)
                print(f'Successfully downloaded {url}\n')
            else:
                print(f'Failed to download {url}\n')
                all_repos_successfully_downloaded = False
                CloningProcess.delete_repo(downloading_folder_path/sub_folder_name)
                unsuccessful_urls.append(url)

        if all_repos_successfully_downloaded:
            print('\n---------- All repositories downloaded successfully ----------\n')
        else:
            print('\nThese are urls which failed.')
            for i, failed_url in enumerate(unsuccessful_urls):
                print(f'{i+1}. {failed_url}')

            print('\n---------- Failed to download some repositories ----------\n')


if __name__ == '__main__':
    CloningProcess().run()

import glob
import logging
import os
import pathlib
import re
from logging.handlers import RotatingFileHandler

from scrapy.utils.log import configure_logging
from selenium import webdriver
from selenium.common import JavascriptException, TimeoutException, NoSuchWindowException
from selenium.webdriver.common.by import By
import time
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import ChromiumOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils import get_collected_links, get_downloaded_urls

BASE_DIR = pathlib.Path(__file__).parent.resolve()

LOG_ENABLED = False

# Disable default Scrapy log settings.
configure_logging(install_root_handler=False)
log_file = BASE_DIR / 'outputs/downloading_zips.log'

root_logger = logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

rotating_file_log = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=1)
rotating_file_log.setFormatter(formatter)

console_log = logging.StreamHandler()
console_log.setLevel(logging.INFO)
console_log.setFormatter(formatter)

root_logger.addHandler(rotating_file_log)
root_logger.addHandler(console_log)


class DownloadGitZips:
    def __init__(self, download_path):
        self.wd = DownloadGitZips.get_webdriver(download_path=download_path)
        root_logger.debug("Starting Webdriver.")

    @classmethod
    def get_webdriver(cls, download_path):
        options = ChromiumOptions()
        options.add_argument("--start-maximized")
        options.add_experimental_option('prefs', {'download.default_directory': download_path})

        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )

    def get_downloaded_filename(self, max_wait_time):
        try:
            self.wd.switch_to.window(self.wd.window_handles[0])
        except NoSuchWindowException as e:
            print(f'Error in get_downloaded_filename: NoSuchWindowException: {e}')
            return
        time.sleep(3)
        end_time = time.time() + max_wait_time
        while True:
            try:
                # get downloaded percentage
                download_percentage = self.wd.execute_script(
                    "return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('#progress').value")
                # check if downloadPercentage is 100 (otherwise the script will keep waiting)
                if download_percentage == 100:
                    # return the file name once the download is completed
                    return self.wd.execute_script(
                        "return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content #file-link').text")
            except JavascriptException as e:
                print(f'Error in get_downloaded_filename: JavascriptException: {e}')

            time.sleep(1)
            if time.time() > end_time:
                try:
                    self.wd.execute_script(
                        "document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#safe>span:nth-child(6)>cr-button').click()"
                    )
                except JavascriptException as e:
                    print('Cant stop downoad')
                    print(f'Error in get_downloaded_filename: JavascriptException: {e}')
                break

    def download_file(self, url, max_wait_time):
        try:
            self.wd.execute_script("window.open()")
            self.wd.switch_to.window(self.wd.window_handles[-1])
            self.wd.get(url)
            element = 'get-repo details>summary'
            WebDriverWait(self.wd, 2).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, element))
            )
            self.wd.find_element(By.CSS_SELECTOR, element).click()

            element = '//div[@id="local-panel"]/ul/li[2]/a'
            WebDriverWait(self.wd, 2).until(
                EC.presence_of_element_located((By.XPATH, element))
            )
            self.wd.find_element(By.XPATH, element).click()
        except JavascriptException as e:
            root_logger.info(f'Error in download_file: {e}')
            return
        except TimeoutException as e:
            root_logger.info(f'Error in download_file: {e}')
            raise TimeoutException

        file_name = self.get_downloaded_filename(max_wait_time=max_wait_time)
        time.sleep(2)
        self.wd.switch_to.window(self.wd.window_handles[1])
        self.wd.close()
        self.wd.switch_to.window(self.wd.window_handles[0])

        return file_name

    @classmethod
    def save_file_number(cls, file_number):
        file_path = BASE_DIR / 'outputs/last_file_number.txt'

        with open(file_path, 'w') as file:
            file.write(str(file_number))

    @classmethod
    def rename_file(cls, old_file_name, new_file_name, file_number):
        old_name = BASE_DIR / 'RepoDownloads' / old_file_name
        new_name = BASE_DIR / 'RepoDownloads' / f'{new_file_name}_N{file_number}.zip'
        os.rename(old_name, new_name)
        return f'{new_file_name}_N{file_number}.zip'

    @classmethod
    def get_repository_name(cls, url):
        url = url if url[-1] != "/" else url[:-1]
        return url.split('/')[-1]

    @classmethod
    def downloaded_link(cls, link, starting_number):
        downloaded_link_file_path = BASE_DIR / 'outputs/downloaded_link.txt'
        with open(downloaded_link_file_path, 'a') as file:
            file.write(link + '\n')
        DownloadGitZips.save_file_number(starting_number)

    @classmethod
    def get_repo_urls(cls):
        return get_collected_links() - get_downloaded_urls()

    def run(self, repository_urls, max_wait_time=180, starting_number=1):
        self.wd.get('chrome://downloads')

        for url in repository_urls:
            root_logger.info(f"{starting_number}. {url} | Started Downloading file..")
            try:
                file_name = self.download_file(url=url, max_wait_time=max_wait_time)

                assert file_name is not None, 'File not downloaded properly.'

                new_file_name = DownloadGitZips.get_repository_name(url)
                new_file_name = DownloadGitZips.rename_file(file_name, new_file_name, starting_number)
                message = f'File downloaded successfully with name {new_file_name}'
                root_logger.info(message)
                DownloadGitZips.downloaded_link(url, starting_number)
            except AssertionError:
                message = f'{url} is taking too much time. > {max_wait_time} sec'
                root_logger.error(f"File downloading failed. Error: {message}")
                DownloadGitZips.save_file_number(starting_number)
            except TimeoutException:
                message = f'{url} is invalid'
                root_logger.error(f"File downloading failed. Error: {message}")
                DownloadGitZips.save_file_number(starting_number)

            starting_number += 1

        self.wd.quit()
        message = 'Completed Downloading all files.'
        root_logger.info(message)


def get_last_file_number():
    file_path = BASE_DIR / 'outputs/last_file_number.txt'

    if not os.path.exists(file_path):
        return 0

    with open(file_path, 'r') as file:
        return int(file.read())


class CleanUp:
    def __init__(self):
        message = 'Started Cleanup.'
        root_logger.debug(message)

        folder_path = BASE_DIR / 'RepoDownloads'
        dir_list = os.listdir(folder_path)
        for file_name in dir_list:
            if not os.path.isdir(folder_path / file_name):
                file_name = str(file_name)
                if not CleanUp.is_file_valid(file_name):
                    message = f'Removing file : {file_name}.'
                    root_logger.info(message)

                    os.remove(folder_path / file_name)

    @classmethod
    def is_file_valid(cls, file_name):
        pattern = r'w?_N[0-9]+.zip'
        return bool(re.search(pattern, file_name))


if __name__ == '__main__':

    # This will remove all unfinished zip files
    CleanUp()

    maximum_wait_time = 30  # 180 seconds

    links = DownloadGitZips.get_repo_urls()
    root_logger.info(f"Total links found: {len(links)}")

    path = str(BASE_DIR / 'RepoDownloads')
    last_number = get_last_file_number()
    root_logger.debug(f"Last file number is {last_number}")
    DownloadGitZips(download_path=path).run(links, max_wait_time=maximum_wait_time, starting_number=last_number+1)
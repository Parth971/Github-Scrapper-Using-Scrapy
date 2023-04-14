import json
import os
import pathlib
import sqlite3
from tabulate import tabulate


BASE_DIR = pathlib.Path(__file__).parent.resolve()


def get_all_data():
    scraped_db_file_path = BASE_DIR / 'outputs/scraped.db'
    unique_collected_links_file_path = BASE_DIR / 'outputs/unique_collected_links.txt'

    connection = sqlite3.connect(scraped_db_file_path)
    cursor = connection.cursor()

    res = cursor.execute(f'''SELECT * FROM scraped;''')

    raw_data = set()
    for i in res:
        raw_data.add(i[1])

    with open(unique_collected_links_file_path, 'w') as file:
        for url in raw_data:
            file.write(f"{url[:-4]}\n")

    print(f'\nTotal links: {len(raw_data)}')
    print(f'Created unique_collected_links.txt file.')

    cursor.close()
    connection.close()

    return raw_data


def get_collected_links():
    raw_data = set()

    collected_links_file_path = BASE_DIR / 'outputs/collected_links.txt'
    if not os.path.exists(collected_links_file_path):
        return raw_data

    with open(collected_links_file_path, 'r') as file:
        for url in file.readlines():
            raw_data.add(url.strip())

    return raw_data



def get_downloaded_urls():
    raw_data = set()

    downloaded_link_file_path = BASE_DIR / 'outputs/downloaded_link.txt'
    if not os.path.exists(downloaded_link_file_path):
        return raw_data

    with open(downloaded_link_file_path, 'r') as file:
        for url in file.readlines():
            raw_data.add(url.strip())

    return raw_data

def count_data():
    topic_file_path = BASE_DIR / 'outputs/topic.json'
    if os.path.exists(topic_file_path):
        count_topics()
        return

    scraped_db_file_path = BASE_DIR / 'outputs/scraped.db'
    connection = sqlite3.connect(scraped_db_file_path)
    cursor = connection.cursor()

    res = cursor.execute(f'''SELECT COUNT(*) FROM scraped;''')
    count = 0
    for i in res:
        count = i[0]

    print(f'Total scraped urls is {count}.')

    cursor.close()
    connection.close()


def count_topics():
    topic_file_path = BASE_DIR / 'outputs/topic.json'

    with open(topic_file_path, 'r') as file:
        json_obj = json.load(file)

    search_topics_specific_object = json_obj['search_topics_specific']

    scraped_data = []
    urls_count = 0
    for topic_name, value in search_topics_specific_object.items():
        scraped_data.append([topic_name, value['count']])
        urls_count += value['count']

    print(tabulate(scraped_data, headers=["Topic Name", "URL\'s scraped"]))

    count = len(json_obj['search_topics_specific'])
    print(f'\nTotal topics scraped are {count}.')
    print(f'\nTotal URL\'s scraped are {urls_count}.')


if __name__ == '__main__':
    try:
        count_data()
    except Exception as e:
        print('Invalid Operation!')
        print(f'Error: {e}\n')





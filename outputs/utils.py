import pathlib
import sqlite3

BASE_DIR = pathlib.Path(__file__).parent.parent.resolve()


def get_all_data():
    # file is 100% existing so just get count of rows
    scraped_db_file_path = BASE_DIR / 'outputs/scraped.db'
    connection = sqlite3.connect(scraped_db_file_path)
    cursor = connection.cursor()

    res = cursor.execute(f'''SELECT * FROM scraped;''')
    for i in res:
        print(i, '>>>>>')
    cursor.close()
    connection.close()


def clear_all_data():
    # file is 100% existing so just get count of rows
    scraped_db_file_path = BASE_DIR / 'outputs/scraped.db'
    connection = sqlite3.connect(scraped_db_file_path)
    cursor = connection.cursor()

    cursor.execute(f'''DELETE FROM scraped;''')

    connection.commit()
    cursor.close()
    connection.close()

get_all_data()
# clear_all_data()
import time
import os
import configparser
import argparse
from queue import Queue
from threading import Thread
import math

from tqdm import tqdm
# from multiprocessing import Process, Queue

from database_parsing import DB_channel, DB_videos
from parsing_channel_data import ParserChannelData
from parser_list import ParserList
from csv_convert import convert
from addition_parsing import ParserEmailsInLink

config = configparser.ConfigParser()
config.read('config.ini')

section_name = 'Browser parameters'

PATH_CHROMEDRIVER = config.get(section_name, 'path_chromedriver')
TIME_PARSING = int(config.get(section_name, 'time_parsing'))
USER_DATA_DIR = config.get(section_name, 'user_data_browser')
NUM_THREADS = int(config.get(section_name, 'num_threads'))

# print(os.path.isfile(PATH_CHROMEDRIVER))

q = Queue()


def check_queue():
    print(f'Обработано {q.qsize()} ссылок')


def refine_channel(links):
    for link in links:
        parser = ParserChannelData(url=link)
        list_data = parser.scrap_data()
        q.put(list_data)
        check_queue()


def main(request):
    STEP_ONE = ParserList(url=request,
                          path_chromedriver=PATH_CHROMEDRIVER,
                          time_parsing=TIME_PARSING,
                          user_data_profile=USER_DATA_DIR)
    text = STEP_ONE.scan()
    STEP_ONE.get_links(text)
    print(f'Найдено {len(STEP_ONE.links)} каналов')
    time.sleep(0.2)

    db = DB_channel('test.db')
    db_video = DB_videos('test.db')
    db.create_table()
    threads = []

    num = math.floor(len(STEP_ONE.links) / NUM_THREADS)

    chunks = [STEP_ONE.links[i:i + num] for i in range(0, len(STEP_ONE.links), num)]

    if NUM_THREADS == 1:
        chunks = STEP_ONE.links
    elif len(chunks) > NUM_THREADS:
        chunks[-2] = chunks[-2] + chunks[-1]
        chunks.pop()

    for parser_thread in range(NUM_THREADS):
        threads.append(Thread(target=refine_channel, args=(chunks[parser_thread],)))

    # check_thread = Thread(target=check_queue)

    for parser_thread in threads:
        parser_thread.start()

    for parser_thread in threads:
        parser_thread.join()

    q.put(None)

    print('Вносим данные...')

    while True:
        data = q.get()
        if data is None:
            break

        for video in data[0]:
            db_video.create_table(video[1])
            db_video.check_and_update(video, video[1])

        db = DB_channel('test.db')
        db.create_table()
        db.check_and_update(data[1])

    # for link in tqdm(STEP_ONE.links, desc='Сбор данных со страниц', unit='ссылки'):
    #     parser = ParserChannelData(url=link)
    #     list_data = parser.scrap_data()
    #     # print(list_data[9].split(','))
    #     # email_parser = ParserEmailsInLink(links=list_data[9].split(','),
    #     #                    path_chromedriver=r'C:\Projects\Parsing_NEW\driver\chrome\chromedriver.exe',
    #     #                    time_parsing=5,
    #     #                    user_data_profile='')
    #     # emails = list_data[10].split(',')
    #     # if emails[0] == '' and len(emails) == 1:
    #     #     add_emails = email_parser.scan()
    #     #     print(f'add_emails = {add_emails}')
    #     #     for email in add_emails:
    #     #         emails.append(email)
    #     #     # print(f'emails = {emails}')
    #     #     emails = list(set(emails))
    #     #     emails = ', '.join(x for x in emails)
    #     #     list_data[10] = emails
    #     db.check_and_update(list_data)

    # Получить список файлов в текущей директории
    files = os.listdir()

    # Перебрать все файлы и удалить все JSON файлы
    for file in files:
        if file.endswith(".json"):
            os.remove(file)

    print('Дело сделано!')


def run(url):
    # parser = argparse.ArgumentParser()
    # parser.add_argument('SEARCH_URL', help='Link to the page with the video on YouTube, '
    #                                        'for example https://www.youtube.com/')
    # args = parser.parse_args()

    main(url)

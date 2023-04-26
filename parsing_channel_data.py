import logging
import re
import requests
from bs4 import BeautifulSoup
import json
import datetime
import time
import numpy as np
from csv_convert import convert

# Конфигурация логгера
from tqdm import tqdm

from version_2_0.database_parsing import DB_channel, DB_videos

logger = logging.getLogger('CHANNEL_DATA')
logger.setLevel(logging.DEBUG)

# Определение обработчика файловых логов
file_handler = logging.FileHandler('CHANNEL_DATA.log', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)

# Определение обработчика вывода на консоль
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.INFO)

# Определение форматирования строк логов
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
# console_handler.setFormatter(formatter)

# Добавление обработчиков в логгер
logger.addHandler(file_handler)


# logger.addHandler(console_handler)


class ParserChannelData:
    def __init__(self, url):
        self.main_url = url
        self.time_between_scroll = 0.5
        self.time_between_steps = 8
        self.json_name = url.split('/')[-1:][0] + '.json'

    @staticmethod
    def pars_subs(dictionary_data: dict):

        amount_subs = dictionary_data['header']['c4TabbedHeaderRenderer']['subscriberCountText']['simpleText']
        if 'M' in amount_subs or 'K' in amount_subs:
            clear_text = amount_subs.split(' ')
            liter = clear_text[0][-1:]
            amount = float(clear_text[0][:-1])
            if 'K' in liter:
                amount *= 1000
            elif 'M' in liter:
                amount *= 1000000
        else:
            clear_text = amount_subs.split(' ')
            amount = float(clear_text[0].replace(',', '.'))
        return int(amount)

    @staticmethod
    def pars_name(dictionary_data):
        name = ''
        for i in dictionary_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][:-1]:
            if 'About' in i['tabRenderer']['title']:
                name = i['tabRenderer']['content'] \
                    ['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0] \
                    ['channelAboutFullMetadataRenderer']['title']['simpleText']
        return name

    @staticmethod
    def pars_videos(dictionary_data):
        amount_videos = dictionary_data['header']['c4TabbedHeaderRenderer']['videosCountText']['runs'][0]['text']
        if 'M' in amount_videos or 'K' in amount_videos:
            clear_text = amount_videos.split(' ')
            liter = clear_text[0][-1:]
            amount = float(clear_text[0][:-1])
            if 'K' in liter:
                amount *= 1000
            elif 'M' in liter:
                amount *= 1000000
        else:
            clear_text = amount_videos.split(' ')
            amount = float(clear_text[0].replace(',', '.'))

        return int(amount)

    @staticmethod
    def pars_date_register(dictionary_data):
        date_register = ''
        for i in dictionary_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][:-1]:
            if 'About' in i['tabRenderer']['title']:
                date_register = i['tabRenderer']['content'] \
                    ['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0] \
                    ['channelAboutFullMetadataRenderer']['joinedDateText']['runs'][1]['text']
        return date_register

    @staticmethod
    def pars_amount_watch(dictionary_data):
        views = ''
        for i in dictionary_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][:-1]:
            if 'About' in i['tabRenderer']['title']:
                views = i['tabRenderer']['content'] \
                    ['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0] \
                    ['channelAboutFullMetadataRenderer']['viewCountText']['simpleText']
        views = int(views.split(' ')[0].replace(',', ''))
        return views

    @staticmethod
    def pars_description(dictionary_data):
        description = ''
        for i in dictionary_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][:-1]:
            if 'About' in i['tabRenderer']['title']:
                description = i['tabRenderer']['content'] \
                    ['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0] \
                    ['channelAboutFullMetadataRenderer']['description']['simpleText']
        return description.replace('\n', ' ')

    @staticmethod
    def pars_country(dictionary_data):
        country = ''
        for i in dictionary_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][:-1]:
            if 'About' in i['tabRenderer']['title']:
                country = i['tabRenderer']['content'] \
                    ['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0] \
                    ['channelAboutFullMetadataRenderer']['country']['simpleText']
        return country.replace('\n', '')

    @staticmethod
    def pars_links(dictionary_data):
        links_meta = []
        for i in dictionary_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][:-1]:
            if 'About' in i['tabRenderer']['title']:
                for link in i['tabRenderer']['content'] \
                        ['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0] \
                        ['channelAboutFullMetadataRenderer']['primaryLinks']:
                    links_meta.append(link['navigationEndpoint']['commandMetadata']['webCommandMetadata']['url'])
        return ', '.join(x for x in links_meta)

    @staticmethod
    def pars_average_views_videos(dictionary_data):
        views = 0
        count_video = 0
        for i in dictionary_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][:-1]:
            if 'Videos' in i['tabRenderer']['title']:
                for video in i['tabRenderer']['content']['richGridRenderer']['contents'][:-1]:
                    views_video = video['richItemRenderer']['content']['videoRenderer']['viewCountText']['simpleText']
                    views_video = float(views_video.split()[0].replace(',', ''))
                    views += views_video
                    count_video += 1
        return int(round(views / count_video))

    @staticmethod
    def pars_email_button(dictionary_data):
        for i in dictionary_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][:-1]:
            if 'About' in i['tabRenderer']['title']:
                info = i['tabRenderer']['content'] \
                    ['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0] \
                    ['channelAboutFullMetadataRenderer']
                if 'businessEmailButton' in info:
                    return 1
                else:
                    return 0
        return -1

    @staticmethod
    def pars_video_links(dictionary_data):
        links = []
        video_link = ''
        for i in dictionary_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][:-1]:
            if 'Videos' in i['tabRenderer']['title']:
                for video in i['tabRenderer']['content']['richGridRenderer']['contents'][:-1]:
                    video_id = video['richItemRenderer']['content']['videoRenderer']['videoId']
                    links.append(f'https://www.youtube.com/watch?v={video_id}')
        return links

    @staticmethod
    def pars_video_likes_dates(dictionary_data):
        primary = dictionary_data['contents']['twoColumnWatchNextResults']['results'] \
            ['results']['contents'][0]['videoPrimaryInfoRenderer']

        secondary = dictionary_data['contents']['twoColumnWatchNextResults']['results'] \
            ['results']['contents'][1]['videoSecondaryInfoRenderer']


        try:
            for i in dictionary_data['contents']['twoColumnWatchNextResults']['results'] \
                    ['results']['contents']:
                if 'itemSectionRenderer' in i and len(i['itemSectionRenderer']) == 3:
                    comments = i['itemSectionRenderer']['contents'][0]['commentsEntryPointHeaderRenderer'] \
                        ['commentCount']['simpleText']

            if 'M' in comments or 'K' in comments:
                clear_text = comments.split(' ')
                liter = clear_text[0][-1:]
                amount = float(clear_text[0][:-1])
                if 'K' in liter:
                    amount *= 1000
                elif 'M' in liter:
                    amount *= 1000000
            else:
                clear_text = comments.split(' ')
                amount = float(clear_text[0].replace(',', '.'))

        except Exception as exc:
            logger.error(exc.__str__())
            amount = 0

        views = int(
            primary['viewCount']['videoViewCountRenderer']['viewCount']['simpleText'].split(' ')[0].replace(',', ''))

        date_pub = primary['dateText']['simpleText'].replace('Premiered on ', '')
        date_video = datetime.datetime.strptime(date_pub, '%d %b %Y')
        likes_video = float(primary['videoActions']['menuRenderer']['topLevelButtons'][0] \
                                ['segmentedLikeDislikeButtonRenderer']['likeButton']['toggleButtonRenderer'] \
                                ['defaultText']['accessibility']['accessibilityData']['label'].replace(',', '').split(
            'likes')[0])

        name = secondary['owner']['videoOwnerRenderer']['title']['runs'][0]['navigationEndpoint']['commandMetadata']['webCommandMetadata']['url'][2:]
        return date_video, likes_video, amount, views, name,

    @staticmethod
    def pars_keywords(dictionary_data):
        keywords = dictionary_data['metadata']['channelMetadataRenderer']['keywords']
        return keywords

    def scrap_data(self):

        logger.info('Начало сканирования')

        cookies = {"VISITOR_INFO1_LIVE": "vaIgXbQ-AR8", "PREF": "f4",
                   "SID": "VAieiahBk8CzaTBRyTbAkSloY8l3bEhj1eUEuVb9qkMAdrzfUo2mS-Yk0tHa7GsziCTH7g.",
                   "__Secure-1PSID": "VAieiahBk8CzaTBRyTbAkSloY8l3bEhj1eUEuVb9qkMAdrzfUBfLyZPTTriasvjU8MBtLw.",
                   "__Secure-3PSID": "VAieiahBk8CzaTBRyTbAkSloY8l3bEhj1eUEuVb9qkMAdrzf78QsZlSYj0QRB3gUbk0lAg.",
                   "HSID": "AGCVhalTo41KAZM7W", "SSID": "AobuWeIduZUoU9cfn",
                   "APISID": "4eLyz3MEl8EqNhqm/A3l1vAWbZcwu19AXr", "SAPISID": "xwwg3j9PA5Leul9r/AXTC6eqIJ5KzfGgF0",
                   "__Secure-1PAPISID": "xwwg3j9PA5Leul9r/AXTC6eqIJ5KzfGgF0",
                   "__Secure-3PAPISID": "xwwg3j9PA5Leul9r/AXTC6eqIJ5KzfGgF0",
                   "LOGIN_INFO": "AFmmF2swRAIgTnuqPMYbUeoFo9o-B9pVjyRNCUqe_wIyvC29wqJts9ICIB7R7DYRc2yl81bWfEb93iGjZB-1gf9et_G4P7yYy1iQ:QUQ3MjNmeDhaLXBJeExKNkNrOE5xSW5oXzhJY0lMSWY4ZVdBUmxabWNyRVljLTJPU3pKdTZPQkt6R21DMDhXVUxzZmZoYnpmM1JVbG5OeTJCYW9ueGxQOWVvSHRUYUU4blpjMjM2VGladE9DLVUydlJiX0RIRE1uMUI4WUZkVy12QTl5RHRuUlFSWThfcWY4Yk5HZzI2M2ZQN0xVa3g1akV3",
                   "YSC": "-UNtW84XKEg",
                   "SIDCC": "AP8dLtzQQLc3zR5xwV45b6KB8SPX-EVrCz4Zmv2hLw_wdjUUtn4CB1UVoWVv3stGnernA0rB",
                   "__Secure-1PSIDCC": "AP8dLtzprByTsp2CqotzGMGKcJH8d1BFtPbQnXBVZ0hWm9PYXjhqFNTW6l6cH4uZPb6C1zDP",
                   "__Secure-3PSIDCC": "AP8dLtyV2GWCoVXD1tULSYZdCSrNFMX-VWribPnl7oLkYaF_FQcQipQC0AQ8FMNCQDXVi7ns"}
        response_about = requests.get(self.main_url + '/about', cookies=cookies)
        response_main = requests.get(self.main_url + '/videos', cookies=cookies)

        soup = BeautifulSoup(response_about.text, 'lxml')
        soup_main = BeautifulSoup(response_main.text, 'lxml')

        scripts = soup.find_all('script')
        for script in scripts:
            if 'responseContext' in script.text:
                text = script.text[20:-1]
                with open(self.json_name, 'w', encoding='utf8') as file:
                    file.write(text)
                with open(self.json_name, 'r', encoding='utf8') as file:
                    html = json.load(file)

        scripts_main = soup_main.find_all('script')
        for script in scripts_main:
            if 'responseContext' in script.text:
                text = script.text[20:-1]
                with open(self.json_name, 'w', encoding='utf8') as file:
                    file.write(text)
                with open(self.json_name, 'r', encoding='utf8') as file:
                    html_main = json.load(file)

        link = self.main_url

        name = ''
        subs = 0
        videos = 0
        date_register = ''
        watch = 0
        description = ''
        country = ''
        links = ''

        video_data = []
        video_links = []

        email_button = False

        emails = list(set(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', response_about.text)))

        try:
            name = self.pars_name(html)
            logger.info(f'Получено имя ({name})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            subs = self.pars_subs(html)
            logger.info(f'Получено количество подписчиков ({subs})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            videos = self.pars_videos(html)
            logger.info(f'Получено количество видео ({videos})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            date_register = self.pars_date_register(html)
            logger.info(f'Получена дата регистрации ({date_register})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            watch = self.pars_amount_watch(html)
            logger.info(f'Получено количество просмотров ({watch})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            description = self.pars_description(html)
            logger.info(f'Получено описание ({description})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            country = self.pars_country(html)
            logger.info(f'Получена страна ({country})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            links = self.pars_links(html)
            logger.info(f'Получены ссылки ({links})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            avg_views = self.pars_average_views_videos(html_main)
            logger.info(f'Получено среднее число просмотров ({avg_views})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            email_button = self.pars_email_button(html)
            logger.info(f'Есть кнопка или нет - найдет ответ! ({email_button})')
        except Exception as exc:
            logger.error(exc.__str__())

        try:
            video_links = self.pars_video_links(html_main)
            logger.info(f'Получены ссылки на видосы {len(video_links)}')
        except Exception as exc:
            logger.error(exc.__str__())

        likes = []
        dates = []
        comments = []
        for video in video_links:
            try:
                video_response = requests.get(video)
                video_soup = BeautifulSoup(video_response.text, 'lxml')

                scripts = video_soup.find_all('script')
                for script in scripts:
                    if 'var ytInitialData = {"responseContext":' in script.text:
                        text = script.text[20:-1]
                        with open('video.json', 'w', encoding='utf8') as file:
                            file.write(text)
                        with open('video.json', 'r', encoding='utf8') as file:
                            video_dict = json.load(file)

                data = self.pars_video_likes_dates(video_dict)
                # print(f'{self.main_url} --> {data[0]}, {data[1]}, {data[2]}, {data[3]}')
                dates.append(data[0])
                likes.append(data[1])
                comments.append(data[2])

                video_data.append((video, data[4], 'XXX', data[0], data[3], data[2], data[1]))

            except Exception as exc:
                logger.error(f'{exc.__str__()} on {video}')

        kw = self.pars_keywords(html)
        logger.info('Сканирование завершено!')

        return [video_data, (link, name, subs, videos, date_register, watch, description, country, links,
                ', '.join(x for x in emails), email_button, kw)]


if __name__ == '__main__':
    a = ParserChannelData(url='https://www.youtube.com/@naveenautomationlabs')
    db_video = DB_videos('test.db')

    data = a.scrap_data()
    print(data)

    for video in data[0]:
        print(video[1])
        db_video.create_table(video[1])
        db_video.check_and_update(video, video[1])

    db = DB_channel('test.db')
    db.create_table()
    db.check_and_update(data[1])
    convert('ready.csv', 'test.db', 'channels')

from selenium.webdriver import Keys
from selenium.webdriver.chrome.service import Service
import time
import datetime
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
import random
import requests
from bs4 import BeautifulSoup
import logging
import speech_recognition as sr
import os
from pydub import AudioSegment
from selenium.webdriver.common.action_chains import ActionChains
import numpy as np
import re
from utils import get_random_account, get_random_proxy, bypass_captcha
import shutil

# user_agent_data = [
#     'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
# ]

user_agent_data = [
"Mozilla/5.0 (Linux; Android 12; SM-M115F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36",
"Mozilla/5.0 (Linux; Android 12; SM-M127F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36",
"Mozilla/5.0 (Linux; Android 12; SM-M215F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36"
]
# Конфигурация логгера
logger = logging.getLogger('ParserRecaptcha')
logger.setLevel(logging.DEBUG)

# Определение обработчика файловых логов
file_handler = logging.FileHandler('ParserRecaptcha.log', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)

# Определение обработчика вывода на консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Определение форматирования строк логов
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Добавление обработчиков в логгер
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def error_message(message):
    with open('crash.log', 'a', encoding='utf-8') as crash_file:
        crash_file.write(f'{datetime.datetime.now()}\n\n')
        crash_file.write(message)
        crash_file.write('\n\n')


class Parser:
    def __init__(self, url, path_chromedriver, browser_data):
        self.main_url = url
        self.name = url.split('/')[-2:-1][0][1:].replace('.', '_')
        self.path_exe = path_chromedriver
        self.user_data_profile = browser_data
        self.mouse_position = [0, 0]
        self.scroll = 0
        self.flag_access = True
        self.status_block = False
        self.cookie = {}
        self.time_between_steps = 10
        self.driver = self.initial_driver()
        self.email_button = '/html/body/ytd-app/div[1]/ytd-page-manager/' \
                            'ytd-browse/ytd-two-column-browse-results-re' \
                            'nderer/div[1]/ytd-section-list-renderer/div' \
                            '[2]/ytd-item-section-renderer/div[3]/ytd-ch' \
                            'annel-about-metadata-renderer/div[1]/div[4]' \
                            '/table/tbody/tr[1]/td[3]/ytd-button-rendere' \
                            'r/yt-button-shape/button'
        self.iframe_recaptcha_xpath = '/html/body/ytd-app/div[1]/ytd-pag' \
                                      'e-manager/ytd-browse[2]/ytd-two-c' \
                                      'olumn-browse-results-renderer/div' \
                                      '[1]/ytd-section-list-renderer/div' \
                                      '[2]/ytd-item-section-renderer/div' \
                                      '[3]/ytd-channel-about-metadata-re' \
                                      'nderer/div[1]/div[4]/table/tbody/' \
                                      'tr[1]/td[4]/div/div/div/iframe'
        self.recaptcha_xpath = '/html/body/div[2]/div[3]'
        self.music_element = '/html/body/div/div/div[3]/div[2]/div[1]/di' \
                             'v[1]/div[2]/button'
        self.recaptcha_xpath_new = '/html/body/div/div[4]/iframe'
        self.input_audio_answer = '/html/body/div/div/div[6]/input'
        self.submit_button = '/html/body/ytd-app/div[1]/ytd-page-manager/ytd-browse/ytd-two-column-browse-results-renderer/div[1]/ytd-section-list-renderer/div[2]/ytd-item-section-renderer/div[3]/ytd-channel-about-metadata-renderer/div[1]/div[4]/table/tbody/tr[1]/td[4]/button/span/yt-formatted-string'
        self.email_ready = '/html/body/ytd-app/div[1]/ytd-page-manager/y' \
                           'td-browse[2]/ytd-two-column-browse-results-r' \
                           'enderer/div[1]/ytd-section-list-renderer/div' \
                           '[2]/ytd-item-section-renderer/div[3]/ytd-cha' \
                           'nnel-about-metadata-renderer/div[1]/div[4]/t' \
                           'able/tbody/tr[1]/td[6]/a'
        self.recaptcha_access = '/html/body/div[2]/div[1]'

    def initial_driver(self):

        options = webdriver.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(f"user-agent={random.choice(user_agent_data)}")
        options.add_argument('--headless')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        shutil.rmtree(self.user_data_profile)
        options.add_argument(f"user-data-dir={self.user_data_profile}")
        options.add_argument("window-size=800,600")
        # options.add_extension(get_random_proxy())
        proxy_options = get_random_proxy()
        logger.info(f'Подключаюсь через прокси - {proxy_options["proxy"]["http"].split("@")[1]}')

        service = Service(executable_path=self.path_exe)
        driver = webdriver.Chrome(service=service, options=options, seleniumwire_options=proxy_options)

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            'source': '''
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        '''
        })
        return driver

    def save_to_file(self):
        page = self.driver.page_source
        with open(f'{self.name}.html', 'w', encoding='utf-8') as file:
            file.write(page)

    def get_url_audio(self):
        page = ''
        with open(f'{self.name}.html', 'r', encoding='utf-8') as file:
            page = page.join(file.read())

        soup = BeautifulSoup(page, 'lxml')
        try:
            tag_link = soup.find('a', class_='rc-audiochallenge-tdownload-link')
            if tag_link is None:
                return tag_link
        except Exception:
            return None
        return tag_link.get_attribute_list('href')

    def get_mp3_file(self, url_audio):
        response = requests.get(url_audio)

        with open(f'{self.name}.mp3', 'wb') as file:
            file.write(response.content)

    def convert_mp3_to_wav(self):
        slice = len(self.name) + 4

        mp3_file = os.path.abspath(f'{self.name}.mp3')
        wav_file = os.path.join(mp3_file[:-slice], f'{self.name}.wav')

        AudioSegment.from_mp3(mp3_file).export(wav_file, format="wav")

    def recognize_file(self):
        r = sr.Recognizer()

        with sr.AudioFile(f'{self.name}.wav') as source:
            audio = r.record(source)

        text = r.recognize_google(audio)
        os.remove(f'{self.name}.wav')
        os.remove(f'{self.name}.mp3')
        return text

    def hover_mouse(self, element):
        action = ActionChains(self.driver)
        time_to_position = 0.1 + random.random()
        coordinates_element = element.rect
        center = (coordinates_element['x'] + 20,
                  coordinates_element['y'] + 20)

        # вычисление вектора направления
        direction_vector = np.array([center[0] - self.mouse_position[0],
                                     center[1] - self.mouse_position[1] - self.scroll])
        # длина вектора направления
        length = np.linalg.norm(direction_vector)
        # создание массива точек
        num_points = 70
        t = np.linspace(0, length, num_points)
        sinusoid = 10 * random.random() * np.cos(0.1 * t)
        points = np.zeros((num_points, 2))
        for i in range(num_points):
            points[i, :] = np.array([self.mouse_position[0], self.mouse_position[1]]) + t[i] * \
                           direction_vector / length + np.array([0, sinusoid[i]])

        points[num_points - 1, 1] = center[1] - self.scroll
        for x, y in points:
            action.move_by_offset(x - self.mouse_position[0],
                                  y - self.mouse_position[1]).perform()
            self.mouse_position = [x, y]

    def login_random(self):
        logger.info("Захожу в аккаунт")

        account_data = get_random_account()

        # Подключение к странице
        try:
            self.driver.maximize_window()
            self.driver.get('http://accounts.google.com/Login')
            logger.debug('Подключился к странице авторизации')
        except Exception as exc:
            logger.error('Не удалось подключиться к странице авторизации')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

        time.sleep(2 + (self.time_between_steps - 2) * random.random())

        # Нажатие на кнопку входа
        try:
            ActionChains(self.driver).scroll_by_amount(0, 500).perform()
            self.scroll += 500
            list_accounts = self.driver.find_element(By.XPATH,
                                                     value='/html/body/div[1]/div[1]/div[2]/div/div[2]/div/div/div[2]/div/div[1]/div/form/span/section/div/div/div/div/ul')
            change_account = list_accounts.find_elements(By.TAG_NAME, value='li')[-2:-1][0]
            # self.hover_mouse(change_account)
            change_account.click()
            logger.debug('Проскроллил и нашел кнопку, нажал на нее')
        except Exception as exc:
            logger.warning('Не удалось скроллить, найти кнопку или нажать на нее')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")

        time.sleep(2 + (self.time_between_steps - 2) * random.random())

        # Ввод email логина
        try:
            email_elem = self.driver.find_element(By.NAME, value='identifier')
            # self.hover_mouse(email_elem)
            email_elem.click()

            email_elem.send_keys(f'{account_data["email"]}')
            time.sleep(3 * random.random())
            email_elem.send_keys(Keys.ENTER)
            logger.debug('Ввел email')
        except Exception as exc:
            logger.error('Не удалось ввести email')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

        time.sleep(6 + (self.time_between_steps - 6) * random.random())

        # Ввод пароля
        try:
            for i in self.driver.find_elements(By.TAG_NAME, value='input'):
                if i.get_attribute(name='name') == 'password' or i.get_attribute(name='name') == 'Passwd':
                    pass_elem = i
            # self.hover_mouse(pass_elem)
            pass_elem.click()

            pass_elem.send_keys(f'{account_data["password"]}')
            time.sleep(2)
            pass_elem.send_keys(Keys.ENTER)
            logger.debug('Ввел пароль')
        except Exception as exc:
            logger.error('Не удалось ввести пароль')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            time.sleep(100)
            return -1

        time.sleep(2 + (self.time_between_steps - 2) * random.random())

        try:
            li_list = self.driver.find_elements(By.TAG_NAME, value='li')
            li_list[-2:-1][0].click()

            time.sleep(2 + (self.time_between_steps - 2) * random.random())

            for i in self.driver.find_elements(By.TAG_NAME, value='input'):
                if i.get_attribute(name='name') == 'email':
                    pass_elem = i
            # self.hover_mouse(pass_elem)
            pass_elem.click()

            pass_elem.send_keys(f'{account_data["add_email"]}')
            time.sleep(2)
            pass_elem.send_keys(Keys.ENTER)
            logger.debug('Ввел дополнительный email')
        except Exception as exc:
            logger.warning('Не удалось ввести дополнительный email')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")

        time.sleep(2 + (self.time_between_steps - 2) * random.random())

        logger.info(f"Успешно залогинился за {account_data['email']}")

    def sign_out(self):
        logger.info("Выхожу из аккаунта")

        profile_xpath = '/html/body/div[4]/header/div[2]/div[3]/div[1]/div[2]/div/a'
        iframe_present_xpath = '/html/body/div[4]/header/div[2]/div[3]/div[3]/iframe'
        exit_xpath = '/html/body/div/c-wiz/div/div/div/div/div[2]/div[2]/span/a'

        # Подключение к странице
        try:
            self.driver.maximize_window()
            self.driver.get('http://accounts.google.com/Login')
            logger.debug('Подключился к странице авторизации')
        except Exception as exc:
            logger.error('Не удалось подключиться к странице авторизации')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

        time.sleep(2 + (self.time_between_steps - 2) * random.random())

        # Поиск и нажатие на иконку профиля
        try:
            profile_icon = self.driver.find_element(By.XPATH, value=profile_xpath)
            # self.hover_mouse(profile_icon)
            profile_icon.click()
            logger.debug('Нашел кнопку профиля, нажал на нее')
        except Exception as exc:
            logger.error('Не удалось найти или нажать на иконку профиля')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

        time.sleep(2 + (self.time_between_steps - 2) * random.random())

        # Переключение на раздел со сменой аккаунта
        try:
            iframe_present = self.driver.find_element(By.XPATH, value=iframe_present_xpath)
            self.driver.switch_to.frame(iframe_present)
            logger.debug("Переключился на другой iframe")
        except Exception as exc:
            logger.error('Не удалось найти или переклдючиться на другой iframe')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

        time.sleep(2 + (self.time_between_steps - 2) * random.random())

        # Нажатие на кнопку смены аккаунта
        try:
            exit_elem = self.driver.find_element(By.XPATH, value=exit_xpath)
            # self.hover_mouse(exit_elem)
            exit_elem.click()
            logger.debug("Нажал на выход")
        except Exception as exc:
            logger.error('Не удалось нажать на кнопку выхода')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

        time.sleep(2 + (self.time_between_steps - 2) * random.random())
        logger.info("Успешно вышел из аккаунта")

    def connect(self):
        self.driver.maximize_window()
        try:
            self.driver.get(self.main_url)
            logger.debug(f'Успешно подключились к странице {self.main_url}')
            return 0
        except Exception as exc:
            logger.error('Не удалось подключиться к странице или передать куки!')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

    def click_button(self):
        try:
            email = self.driver.find_element(By.XPATH, value=self.email_button)
            ActionChains(self.driver).scroll_by_amount(0, 500).perform()
            self.scroll += 500
            self.hover_mouse(email)
            self.driver.find_element(By.XPATH, value=self.email_button).click()
            logger.debug(f'Нажата кнопка с email')
            return 0
        except Exception as exc:
            self.status_block = True
            logger.error('Не удалось нажать на кнопку!')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

    def change_iframe(self):
        try:
            iframe = self.driver.find_element(By.TAG_NAME, value='iframe')
            self.driver.switch_to.frame(iframe)
            logger.debug(f'Найден iframe recaptcha, перешел в iframe')
            return 0
        except Exception as exc:
            logger.error('Не удалось найти или перейти в iframe recaptcha check-box!')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

    def click_checkbox(self):
        try:
            check_switch_recaptcha = False
            self.driver.switch_to.default_content()
            element = self.driver.find_element(By.XPATH,
                                               value='/html/body/ytd-app/div[1]/ytd-page-manager/ytd-browse[1]/ytd-two-column-browse-results-renderer/div[1]/ytd-section-list-renderer/div[2]/ytd-item-section-renderer/div[3]/ytd-channel-about-metadata-renderer/div[1]/div[4]/table/tbody/tr[1]/td[4]/div')
            self.hover_mouse(element)
            while not check_switch_recaptcha:
                try:
                    self.driver.switch_to.default_content()
                    self.change_iframe()
                    self.driver.find_element(By.XPATH, value=self.recaptcha_xpath).click()
                    logger.debug(f'Клик по check-box recaptcha')
                    check_switch_recaptcha = True
                except Exception:
                    logger.debug(f'Еще одна попытка клика по check-box recaptcha')
                    time.sleep(2)
            return 0
        except Exception as exc:
            logger.error('Не удалось найти или кликнуть по recaptcha check-box!')
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            error_message(exc.__str__())
            return -1

    def check_captcha(self):
        try:
            self.driver.switch_to.default_content()
            self.change_iframe()
            page = self.driver.page_source
            self.save_to_file()
            if 'Вы прошли проверку' not in page:
                return 0
            return 1
        except Exception as exc:
            logger.error('Не удалось найти пройденную капчу')
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            error_message(exc.__str__())
            return -1

    def click_music(self):
        try:
            self.driver.switch_to.default_content()
            iframe_two = self.driver.find_element(By.XPATH, value=self.recaptcha_xpath_new)
            self.driver.switch_to.frame(iframe_two)
            element = self.driver.find_element(By.XPATH, value=self.music_element)
            self.hover_mouse(element)
            element.click()
            logger.debug(f'Переход на другой iframe и клик по кнопке "наушников"')
            return 0
        except Exception as exc:
            logger.error('Не удалось найти iframe или кликнуть по кнопке "наушников"!')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

    def answer_captcha(self, answer):
        try:
            self.driver.find_element(By.XPATH, value=self.input_audio_answer).send_keys(answer)
            logger.debug(f'Ответ вставлен в поле капчи')
            return 0
        except Exception as exc:
            logger.error('Не удалось вставить ответ в поле капчи')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

    def send_answer(self):
        try:
            self.driver.find_element(By.XPATH, value=self.input_audio_answer).send_keys(Keys.ENTER)
            logger.debug(f'Ответ отправлен капче')
            return 0
        except Exception as exc:
            logger.error('Не удалось отправить ответ капче')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

    def click_email_after_captcha(self):
        try:

            button_no = False
            self.driver.switch_to.default_content()
            while not button_no:
                logger.debug(f'Еще одна попытка нажать на кнопку')
                time.sleep(0.2)
                for x in self.driver.find_elements(By.TAG_NAME, value='button'):
                    if x.get_attribute('id') == 'submit-btn':
                        if x.is_displayed():
                            x.click()
                        else:
                            button_no = True

            logger.debug(f'Капча завершена!')
            return 0
        except Exception as exc:
            logger.error('Не удалось завершить капчу')
            error_message(exc.__str__())
            self.driver.save_screenshot(f"{self.name}_error_message.png")
            return -1

    def getting_to_bypass(self):
        html = self.driver.page_source

        soup = BeautifulSoup(html, 'lxml')
        div = soup.find('div', {'id': 'recaptcha'})
        googlekey = div.get('data-sitekey')
        url = self.driver.current_url
        key = '8ac2dad0bef1b90fbe144163a687ea04'
        return bypass_captcha(key, googlekey, url)




    def parsing(self):
        audio_link = ''
        answer = ''

        if self.login_random() == -1:
            logger.error('Неудачный вход')

        self.scroll = 0

        logger.info(f'Начался парсинг страницы {self.main_url}')

        # Коннектимся к странице
        if self.connect() == -1:
            return 1

        time.sleep(2 + (self.time_between_steps - 2) * random.random())

        # Нажимаем на кнопку с мылом
        if self.click_button() == -1:
            return 1

        time.sleep(2 + (self.time_between_steps - 2) * random.random())

        answer_key = self.getting_to_bypass()

        logger.info('Капча решена!')

        self.driver.execute_script('document.getElementById("g-recaptcha-response").removeAttribute("style");')


        # time.sleep(100)
        self.driver.find_element(By.ID, value='g-recaptcha-response').send_keys(answer_key)

        # # Нажимаем на checkbox
        # if self.click_checkbox() == -1:
        #     return 1
        #
        # time.sleep(2 + (self.time_between_steps - 2) * random.random())
        #
        # # Проверяем, прошли капчу сразу или нет
        # check_captcha = self.check_captcha()
        # if check_captcha == -1:
        #     return 1
        # elif check_captcha == 0:
        #
        #     time.sleep(2 + (self.time_between_steps - 2) * random.random())
        #
        #     # Кликаем на кнопку "наушников"
        #     if self.click_music() == -1:
        #         return 1
        #
        #     time.sleep(2 + (self.time_between_steps - 2) * random.random())
        #
        #     # Сохраняем HTML страницу
        #     self.save_to_file()
        #
        #     # Парсим аудио
        #     if self.get_url_audio() is None:
        #         return 1
        #     else:
        #         audio_link += self.get_url_audio()[0]
        #
        #     # Скачиваем аудио
        #     self.get_mp3_file(audio_link)
        #
        #     # Преобразуем аудио в WAV формат
        #     self.convert_mp3_to_wav()
        #
        #     # Распознаем текст в аудио
        #     answer += self.recognize_file()
        #
        #     # Вставляем текст в поле капчи
        #     if self.answer_captcha(answer) == -1:
        #         return 1
        #
        #     time.sleep(2 + (self.time_between_steps - 2) * random.random())
        #
        #     # Отправляем ответ
        #     if self.send_answer() == -1:
        #         return 1
        #
        #     time.sleep(2 + (self.time_between_steps - 2) * random.random())

        # Кликаем на кнопку получения email
        if self.click_email_after_captcha() == -1:
            logger.warning('Что-то пошло не так')
            return 1

        else:

            logger.info('email получен')

            time.sleep(2 + (self.time_between_steps - 2) * random.random())

            # Сохраняем страницу и парсим email
            logger.debug(f'Сохраняю мыло')
            ready_html = self.driver.page_source

            email_main = list(set(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', ready_html)))

            return email_main

    def process(self):
        success = False

        while not success:

            code = self.parsing()

            self.scroll = 0

            if code == 1:

                if self.sign_out() == -1:
                    logger.error('Неудачный выход')

                self.scroll = 0

                if self.login_random() == -1:
                    logger.error('Неудачный вход')

                proxy_options = get_random_proxy()
                self.driver.proxy = proxy_options
                logger.info(f'Подключаюсь через прокси - {proxy_options["proxy"]["http"].split("@")[1]}')

                self.scroll = 0

            else:
                success = True
                time.sleep(10)
                return code

            self.scroll = 0


if __name__ == '__main__':
    a = Parser(url='https://www.youtube.com/@GSTV/about',
               path_chromedriver=r'C:\Projects\Parsing_NEW\driver\chrome\chromedriver.exe',
               browser_data='C:\Projects\Parsing_NEW\check_captcha\selenium')

    ready = a.process()

    with open('out.txt', 'w', encoding='utf8') as file:
        file.write(str(ready))
        file.write('\n\n\n')
        file.write(str(type(ready)))



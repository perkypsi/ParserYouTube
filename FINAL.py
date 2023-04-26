from pars_captcha import Parser
from tqdm import tqdm
import random
import time
from login import login_run
from logout import logout_run

from database_parsing import DB

def main(DB_name, browser_data, path_chromedriver):
    db = DB(DB_name)
    data = db.get_all()
    count_links = 0
    random_count = random.randint(3, 5)
    for data_channel in tqdm(data, desc='Парсинг с кнопкой (прямо как Philip Morris)', unit='channels'):
        data_channel = list(data_channel)
        success = False
        if data_channel[12] == 1 and data_channel[11] == '':
            while not success:
                email_parser = Parser(url=data_channel[1] + '/about',
                                      path_chromedriver=path_chromedriver,
                                      browser_data=browser_data)
                out = email_parser.parsing()
                email_parser.driver.quit()
                if out is None or count_links == random_count:
                    logout_run()
                    login_run()
                    count_links =  0
                    random_count = random.randint(3, 5)
                    print('Не удалось спарсить ссылку')
                else:
                    if len(out) != 0:
                        data_channel[10] = ' '.join(x for x in out)
                    db.check_and_update(data_channel)
                    success = True
                    count_links += 1

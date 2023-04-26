import STEP_1
# import STEP_2
# import FINAL
from csv_convert import convert


if __name__ == '__main__':
    STEP_1.run('https://www.youtube.com/results?search_query=playing+with+son&sp=EgIIBA%253D%253D')
    # STEP_2.main('test.db')
    # FINAL.main(DB_name='test.db',
    #            path_chromedriver=r'C:\Projects\Parsing_NEW\driver\chrome\chromedriver.exe',
    #            browser_data=r'C:\Projects\Parsing_NEW\check_captcha\selenium')

    convert('ready.csv', 'test.db', 'channels')

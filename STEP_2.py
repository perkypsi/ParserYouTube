from tqdm import tqdm

from database_parsing import DB
from addition_parsing import ParserEmailsInLink


def main(DB_name):
    db = DB(DB_name)
    data = db.get_all()
    for data_channel in tqdm(data, desc='Дополнение емейлами', unit='channels'):
        data_channel = list(data_channel)
        email_parser = ParserEmailsInLink(links=data_channel[9].split(','),
                           path_chromedriver=r'C:\Projects\Parsing_NEW\driver\chrome\chromedriver.exe',
                           time_parsing=5,
                           user_data_profile='')
        emails = data_channel[10].split(',')
        if emails[0] == '' and len(emails) == 1:
            add_emails = email_parser.scan()
            for email in add_emails:
                emails.append(email)
            emails = list(set(emails))
            emails = ', '.join(x for x in emails)
            data_channel[10] = emails
            db.check_and_update(data_channel)
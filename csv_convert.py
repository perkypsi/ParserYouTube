import csv
import sqlite3


def convert(csv_filename, db_filename, table_name):
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    cursor.execute(f'''SELECT * FROM {table_name}''')
    rows = cursor.fetchall()
    with open(csv_filename, 'w', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=';')
        csv_writer.writerow(['ID', 'LINK', 'NAME', 'AMOUNT VIDEOS', 'AMOUNT SUBS', 'DATE OF REGISTRATION',
                             'NUMBER OF VIEWS', 'AVERAGE VIEW NUMBER', 'DESCRIPTION', 'COUNTRY', 'LINKS', 'EMAILS',
                             'EMAIL BUTTON', 'KEYWORDS', 'FREQUENCY OF VIDEO PUBLICATION',
                             'AVERAGE AMOUNT OF LIKES PER VIDEO', 'AVERAGE AMOUNT OF COMMENTS PER VIDEO'])
        for row in rows:
            csv_writer.writerow(row)

    conn.commit()
    conn.close()

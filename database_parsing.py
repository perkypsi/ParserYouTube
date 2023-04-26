import sqlite3


class DB_channel:
    def __init__(self, filename):
        self.filename = filename

    def create_table(self):
        conn = sqlite3.connect(self.filename)
        conn.execute('''CREATE TABLE IF NOT EXISTS channels
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     link TEXT NOT NULL,
                     name TEXT NOT NULL,
                     videos INTEGER NOT NULL,
                     subs INTEGER,
                     date_registered TEXT,
                     amount_watch INTEGER,
                     description TEXT,
                     country TEXT,
                     links TEXT,
                     emails TEXT,
                     emailButton INTEGER,
                     kw TEXT
                     );''')
        conn.commit()
        conn.close()

    def insert(self, data: list):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        if len(data) < 12 or len(data) > 12:
            print(f'Данных мало или много для вноса в базу данных ({len(data)})')
            return -1
        clear_data = tuple(data)
        cursor.execute("INSERT INTO channels (link, name, subs, videos, "
                       "date_registered, amount_watch, description, country, links,"
                       "emails, emailButton, kw) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       clear_data)
        conn.commit()
        conn.close()

    def update_field(self, link, field, data):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        try:
            cursor.execute(f"UPDATE channels SET {field}=? WHERE link=?", (data, link))
        except Exception as exc:
            print(exc.__str__())
        conn.commit()
        conn.close()

    def check_and_update(self, data):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        try:
            response = cursor.execute("SELECT * FROM channels WHERE link=?", (data[1],)).fetchall()
            conn.commit()
            conn.close()
            if len(response) != 0:
                self.update_field(data[1], 'name', data[2])
                self.update_field(data[1], 'subs', data[3])
                self.update_field(data[1], 'videos', data[4])
                self.update_field(data[1], 'date_registered', data[5])
                self.update_field(data[1], 'amount_watch', data[6])
                self.update_field(data[1], 'description', data[7])
                self.update_field(data[1], 'country', data[8])
                self.update_field(data[1], 'links', data[9])
                self.update_field(data[1], 'emails', data[10])
                self.update_field(data[1], 'emailButton', data[11])
                self.update_field(data[1], 'kw', data[12])
            else:
                if type(data[0]) == int:
                    self.insert(data[1:])
                else:
                    self.insert(data)
        except Exception as exc:
            print(exc.__str__())

    def get_all(self):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        data = None
        try:
            data = cursor.execute(f"SELECT * FROM channels").fetchall()
        except Exception as exc:
            print(exc.__str__())
        conn.commit()
        conn.close()
        return data

class DB_videos:
    def __init__(self, filename):
        self.filename = filename

    def create_table(self, channel_id):
        conn = sqlite3.connect(self.filename)
        conn.execute(f'''CREATE TABLE IF NOT EXISTS {channel_id}
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     link TEXT NOT NULL,
                     name TEXT NOT NULL,
                     description TEXT,
                     date_registered TEXT,
                     amount_watch INTEGER,
                     amount_comments INTEGER,
                     likes INTEGER
                     );''')
        conn.commit()
        conn.close()

    def insert(self, data: list, channel_id):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        if len(data) < 7 or len(data) > 7:
            print(f'Данных мало или много для вноса в базу данных ({len(data)})')
            return -1
        clear_data = tuple(data)
        cursor.execute(f"INSERT INTO {channel_id} (link, name, description, date_registered, "
                       "amount_watch, amount_comments, likes) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?)",
                       clear_data)
        conn.commit()
        conn.close()

    def update_field(self, link, field, data):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        try:
            cursor.execute(f"UPDATE channels SET {field}=? WHERE link=?", (data, link))
        except Exception as exc:
            print(exc.__str__())
        conn.commit()
        conn.close()

    def check_and_update(self, data, channel_id):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        try:
            response = cursor.execute(f"SELECT * FROM {channel_id} WHERE link=?", (data[1],)).fetchall()
            conn.commit()
            conn.close()
            if len(response) != 0:
                self.update_field(data[1], 'name', data[2])
                self.update_field(data[1], 'description', data[3])
                self.update_field(data[1], 'date_registered', data[4])
                self.update_field(data[1], 'amount_watch', data[5])
                self.update_field(data[1], 'amount_comments', data[6])
                self.update_field(data[1], 'likes', data[7])
            else:
                if type(data[0]) == int:
                    self.insert(data[1:], channel_id)
                else:
                    self.insert(data, channel_id)
        except Exception as exc:
            print(exc.__str__())

    def get_all(self, channel_id):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        data = None
        try:
            data = cursor.execute(f"SELECT * FROM {channel_id}").fetchall()
        except Exception as exc:
            print(exc.__str__())
        conn.commit()
        conn.close()
        return data


# if __name__ == "__main__":
#     db = DB('test.db')
#     db.check_and_update(['https://www.youtube.com/@wsj/aboutl'])

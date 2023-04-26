import pandas as pd
import datetime

from version_2_0.database_parsing import DB_channel, DB_videos



db_video = DB_videos('test.db')
db_channel = DB_channel('test.db')

data_all = db_channel.get_all()

data_all_channel = {
    'Link': [],
    'Name': [],
    'Subs': [],
    'Views total': [],
    'Average amount of views per video': [],
    'Average amount of views per month': [],
    'Description': [],
    'Country': [],
    'Date of registration': [],
    'Frequency of video publication per month': [],
    'Videos total': [],
    'Average amount of comments per video': [],
    'Average amount of likes per video': [],
    'Keywords': [],
    'Social links': []
}

for data_channel in data_all:
    link = data_channel[1]
    channel_id = link.split('/')[-1][1:]
    name = data_channel[2]
    subs = data_channel[4]
    views_total = data_channel[6]
    theme = data_channel[7]
    country = data_channel[8]
    data_registered = data_channel[5]
    videos_total = data_channel[3]
    keywords = data_channel[12]
    social = data_channel[9]

    data_all_channel['Link'].append(link)
    data_all_channel['Name'].append(name)
    data_all_channel['Subs'].append(subs)
    data_all_channel['Views total'].append(views_total)
    data_all_channel['Description'].append(theme)
    data_all_channel['Country'].append(country)
    data_all_channel['Date of registration'].append(data_registered)
    data_all_channel['Videos total'].append(videos_total)
    data_all_channel['Keywords'].append(keywords)
    data_all_channel['Social links'].append(social)

    data = db_video.get_all(channel_id)
    clear_data = {
        'link': [],
        'name': [],
        'description': [],
        'date_pub': [],
        'views': [],
        'comments': [],
        'likes': []
    }

    for row in data:
        clear_data['link'].append(row[1])
        clear_data['name'].append(row[2])
        clear_data['description'].append(row[3])
        clear_data['date_pub'].append(row[4])
        clear_data['views'].append(row[5])
        clear_data['comments'].append(row[6])
        clear_data['likes'].append(row[7])

    df = pd.DataFrame(clear_data)

    df['date_pub'] = pd.to_datetime(df['date_pub'])

    last_month = datetime.datetime.now() - datetime.timedelta(days=30)
    # last_last_month = datetime.datetime.now() - datetime.timedelta(days=60)
    # last_last_last_month = datetime.datetime.now() - datetime.timedelta(days=90)

    current_month_amount_videos_table = df[(df['date_pub'] >= last_month)]

    current_month_amount_videos = current_month_amount_videos_table['date_pub'].count()
    current_month_amount_views = round(current_month_amount_videos_table['views'].mean())
    current_month_amount_comments = round(current_month_amount_videos_table['comments'].mean())
    current_month_amount_likes = round(current_month_amount_videos_table['likes'].mean())

    data_all_channel['Average amount of views per video'].append(current_month_amount_views)
    data_all_channel['Frequency of video publication per month'].append(current_month_amount_videos)
    data_all_channel['Average amount of comments per video'].append(current_month_amount_comments)
    data_all_channel['Average amount of likes per video'].append(current_month_amount_likes)
    data_all_channel['Average amount of views per month'].append(current_month_amount_views *
                                                                 current_month_amount_videos)


ready_table = pd.DataFrame(data_all_channel)

# FILTERS

df_filtered = ready_table[(ready_table['Subs'] > 500000) & (ready_table['Average amount of views per month'] > 5000000)
                          & (ready_table['Frequency of video publication per month'] > 2)]

df_filtered.to_csv('test.csv')

# print(pd.DataFrame(data_all_channel).to_csv('test.csv'))

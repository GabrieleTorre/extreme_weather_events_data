from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector
from tqdm import tqdm
import pandas as pd
import requests
import re


url = "https://eswd.eu/cgi-bin/eswd.cgi"


payload = (
            'BUT_adv_query=submit%2Bquery&date_selected=on&start_date={}&' +
            'query_start_hour=00&end_date={}&query_end_hour=24&mapnum=0&' +
            'PRECIP=on&selected_countries=IT&initial_orography=all&' +
            'min_latitude=&max_latitude=&min_longitude=&max_longitude=&' +
            'qc0=on&qc0%2B=on&qc1=on&qc2=on&lastquery=6354318155&lang=en_0&' +
            'action=advanced_query'
        )


headers = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Content-Type': 'application/x-www-form-urlencoded',
  'Origin': 'https://eswd.eu',
  'Content-Length': '326',
  'Accept-Language': 'en-GB,en;q=0.9',
  'Host': 'eswd.eu',
  'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ' +
                 'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 ' +
                 'Safari/605.1.15'),
  'Referer': 'https://eswd.eu/cgi-bin/eswd.cgi',
  'Accept-Encoding': 'gzip, deflate, br',
  'Connection': 'keep-alive'
}

database = {'user': "admin", 'password': "R0t0l0n3$!",
            "host": "agritech.cuxplwczrxtp.us-east-1.rds.amazonaws.com",
            "port": 3306, "database": "extreme_weather_events"}



def inster_record(x):
    query = ("INSERT INTO rains (id, event, place, country, latitude, longitude, " +
             "precipitation, duration, status, datetime) VALUES " +
             "('{id}', '{event}'," +'"{place}",'+" '{country}', {latitude}, {longitude}, " +
             "'{precipitation}', '{duration}', '{status}', '{datetime}') " +
             "ON DUPLICATE KEY UPDATE event='{event}', place=" +'"{place}", ' +
             "country='{country}', latitude={latitude}, longitude={longitude}, " +
             "duration='{duration}', precipitation='{precipitation}', " +
             "status='{status}', datetime='{datetime}'")
    try:
        cursor.execute(query.format(**x.to_dict()))
    except:
        print(query.format(**x.to_dict()))
        import pdb; pdb.set_trace()


def parse_single_event(event):

    latlon = re.findall("\d+.\d+", event.text)[:2]

    precipit = re.findall("\d+ mm", event.text)
    precipit = precipit[0] if len(precipit) > 0 else None

    duration = re.findall("\d+ hours", event.text)
    duration = duration[0] if len(duration) > 0 else None

    status = event.findAll('b')[-1].text

    cols = ['event', 'place', 'country', 'date', 'hour']

    return {**{'id': event['id']},
            **{k: v.text for k, v in zip(cols, event.findAll('b')[:5])},
            **{'latitude': float(latlon[0]), 'longitude': float(latlon[1]),
               'duration': duration, 'precipitation': precipit,
               'status': status}}


def format_datetime(x):
    string_datetime = '{} {}'.format(x['date'], x['hour'])
    return datetime.strptime(string_datetime, '%d-%m-%Y %H:%M')


def main(start_date, ndays):
    out = []
    for i in tqdm(range(0, ndays)):
        ref_date = (start_date - relativedelta(days=i)).strftime("%d-%m-%Y")
        _data = payload.format(ref_date, ref_date)
        response = requests.request("POST", url, headers=headers, data=_data,
                                    timeout=(3.05, 27))

        soup = BeautifulSoup(response.content, 'html.parser')
        events = soup.findAll('tr', id=True)
        out += [parse_single_event(event) for event in events]

    df = pd.DataFrame(out)
    df['datetime'] = df.apply(format_datetime, axis=1)
    df = df.drop(columns=['date', 'hour'])
    return df


if __name__ == '__main__':
    df = main(datetime.now().date(), 365*7)

    cnx = mysql.connector.connect(**database)
    cursor = cnx.cursor()

    df.apply(lambda x: inster_record(x), axis=1)

    cursor.execute("UPDATE rains SET precipitation = NULL where precipitation = 'None';")
    cursor.execute("UPDATE rains SET duration = NULL where duration = 'None';")

    cnx.commit()
    cnx.close()

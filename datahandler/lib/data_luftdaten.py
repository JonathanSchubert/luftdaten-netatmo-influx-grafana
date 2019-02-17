import os
import re
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from influxdb import DataFrameClient, InfluxDBClient
from bs4 import BeautifulSoup
from io import BytesIO
from zipfile import ZipFile

from lib.data import Data


class Luftdaten(Data):

    def __init__(self):
        Data.__init__(self)


    def set_config(self):
        # Config
        self.remote_data      = 'https://www.madavi.de/sensor/'
        self.remote_file_data = 'https://www.madavi.de/sensor/data_csv/data-{}-{}.csv'
        self.remote_file_list = 'https://www.madavi.de/sensor/csvfiles.php?sensor={}'
        self.station_id       = os.getenv('LD_SENSOR_ID')

        self.influxdb_cfg = {'host':     os.getenv('INFLUX_HOST', 'localhost'),
                             'port':     8086,
                             'user':     os.getenv('INFLUX_USER', 'admin'),
                             'password': os.getenv('INFLUX_PASSWORD', 'admin'),
                             'dbname':   os.getenv('INFLUX_DB_LD', 'luftdaten'),
                             'protocol': 'line'}

        self.sensors = {'Temperature': 'Temp',
                        'Humidity':    'Humidity',
                        'PM2.5':       'SDS_P1',
                        'PM10':        'SDS_P2'}


    def update_data_complete(self):

        # Retrieve complete history of data
        data_complete = self._retrieve_data_complete()

        # Write data to DB
        client = self._get_connection_db()
        self._write_data(client, data_complete)


    def update_data_today(self):

        # Retrieve data
        today = datetime.now().strftime('%Y-%m-%d')
        data = self._retrieve_data_day(today)

        # Write data to DB
        client = self._get_connection_db()
        self._write_data(client, data)


    def _get_period_for_update(self, client):

        # Get period available remote
        days_remote = self._get_period_data_remote()

        # Get period available in DB
        days_db = self._get_period_in_db(client)

        # Calc diff
        days_to_update = list(set(days_remote) - set(days_db))

        return days_to_update


    def _get_files_remote(self):
        print('   Get period of available data...')
        resp = requests.get(self.remote_file_list.format(self.station_id))

        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")
            links = []
            for link in soup.find_all('a'):
                href = link.get('href')
                if 'data' in href:
                    links.append(href)
        else:
            print('Error: Not able to connect to {resp}')
            exit(1)

        return links


    def _get_period_data_remote(self):
        links = self._get_files_remote()

        links_csv = [x for x in links if x[-4:] == '.csv']
        dates_avail = sorted([re.findall("(\d{4}-\d{2}-\d{2})", x)[0] for x in links_csv])
        print('    ... found', dates_avail[0], ' to ', dates_avail[-1])

        return sorted(dates_avail)


    def _retrieve_data_file_zip(self, file):
        print('   Retrieve zip file {}'.format(file))

        remote_url = self.remote_data  + file
        resp = requests.get(remote_url).content
        zipfile = ZipFile(BytesIO(resp))
        csvfiles = zipfile.namelist()

        data = []
        for csvfile in csvfiles:
            if csvfile == 'data-esp8266-10440194-2018-10-01.csv':
                # import pdb; pdb.set_trace()
                continue
            print('      ... {}'.format(csvfile))

            this_data = pd.read_csv(zipfile.open(csvfile), sep=';')
            this_data = this_data.set_index(pd.to_datetime(this_data.Time))
            this_data = this_data[['SDS_P1', 'SDS_P2', 'Temp', 'Humidity']].dropna()
            data.append(this_data)
        data = pd.concat(data)

        return data


    def _retrieve_data_file_csv(self, file):
        print('   Retrieve csv file {}'.format(file))

        remote_url = self.remote_data  + file
        data = pd.read_csv(remote_url, sep=';')
        data = data.set_index(pd.to_datetime(data.Time))
        data_sel = data[['SDS_P1', 'SDS_P2', 'Temp', 'Humidity']].dropna()

        return data_sel


    def _retrieve_data_complete(self):
        print('Retrieve complete {} history...'.format(self.dataname))
        files = self._get_files_remote()
        files_csv = [x for x in files if x[-4:] == '.csv']
        files_zip = [x for x in files if x[-4:] == '.zip']

        data = []
        for file in files_zip:
            this_data = self._retrieve_data_file_zip(file)
            data.append(this_data)

        for file in files_csv:
            this_data = self._retrieve_data_file_csv(file)
            data.append(this_data)
        data = pd.concat(data)

        return data


    def _retrieve_data_day(self, day):
        print('Retrieve {} data for day {}'.format(self.dataname, day))

        remote_url = self.remote_file_data.format(self.station_id, day)
        data = pd.read_csv(remote_url, sep=';')
        data = data.set_index(pd.to_datetime(data.Time))

        # usefull_columns = ['Time', 'SDS_P1', 'SDS_P2', 'Temp', 'Humidity', 'Samples', 'Min_cycle', 'Max_cycle', 'Signal']
        data_sel = data[['SDS_P1', 'SDS_P2', 'Temp', 'Humidity']].dropna()

        return data_sel

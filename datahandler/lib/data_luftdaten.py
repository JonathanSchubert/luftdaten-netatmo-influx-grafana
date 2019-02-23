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
        self.remote_data         = 'https://www.madavi.de/sensor/'
        self.remote_file_data    = 'https://www.madavi.de/sensor/data_csv/data-{}-{}.csv'
        self.remote_file_list    = 'https://www.madavi.de/sensor/csvfiles.php?sensor={}'
        self.station_id          = os.getenv('LD_SENSOR_ID')

        self.local_file_data     = './data/luftdaten_{}.csv'
        self.sensor_file         = './data/sensors_luftdaten.json'

        self.history_start       = '2019-01-01T00:00:00'
        self.hours_update_buffer = 24
        self.update_interval_min = 60 * 2

        self.influxdb_cfg = {'host':     os.getenv('INFLUX_HOST', 'localhost'),
                             'port':     8086,
                             'user':     os.getenv('INFLUX_USER', 'admin'),
                             'password': os.getenv('INFLUX_PASSWORD', 'admin'),
                             'dbname':   os.getenv('INFLUX_DB_LD', 'luftdaten'),
                             'protocol': 'line'}

    def _retrieve_data_period(self, start, end):
        print('Retrieve complete {} history...'.format(self.dataname))
        files_csv, files_zip = self._get_files_remote(start)

        data = []
        for file in files_zip:
            this_data = self._retrieve_data_file_zip(file)
            data.append(this_data)

        for file in files_csv:
            this_data = self._retrieve_data_file_csv(file)
            data.append(this_data)
        data = pd.concat(data)

        data_sep = {}
        for sensor in data.columns:
            data_sep[sensor] = data[[sensor]].dropna()

        return data_sep

    def _get_files_remote(self, start):
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
            print('Error: Not able to connect')
            exit(1)

        files_csv = [x for x in links if x[-4:] == '.csv']
        files_zip = [x for x in links if x[-4:] == '.zip']

        # start = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S')
        files_csv = [x for x in files_csv
                        if datetime.strptime(
                            re.findall("(\d{4}-\d{2}-\d{2})", x)[0], '%Y-%m-%d')
                            >= start]
        files_zip = [x for x in files_zip
                        if datetime.strptime(
                            re.findall("(\d{4}-\d{2})\.zip", x)[0] + '-01', '%Y-%m-%d')
                            >= start]

        return files_csv, files_zip

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
            this_data = this_data[['SDS_P1', 'SDS_P2', 'Temp', 'Humidity']]
            data.append(this_data)
        data = pd.concat(data)

        return data

    def _retrieve_data_file_csv(self, file):
        print('   Retrieve csv file {}'.format(file))

        remote_url = self.remote_data  + file
        data = pd.read_csv(remote_url, sep=';')
        data = data.set_index(pd.to_datetime(data.Time))
        data_sel = data[['SDS_P1', 'SDS_P2', 'Temp', 'Humidity']]

        return data_sel

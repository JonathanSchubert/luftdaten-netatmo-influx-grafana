import os
import re
import time
import requests
import pandas as pd
from datetime import datetime
from influxdb import DataFrameClient, InfluxDBClient
from bs4 import BeautifulSoup
import subprocess

from lib.data import Data


class Netatmo(Data):

    def __init__(self):
        Data.__init__(self)


    def set_config(self):
        # Config
        self.local_file_data = '/usr/src/app/netatmo_data/'

        self.influxdb_cfg = {'host':     os.getenv('INFLUX_HOST', 'localhost'),
                             'port':     8086,
                             'user':     os.getenv('INFLUX_USER', 'admin'),
                             'password': os.getenv('INFLUX_PASSWORD', 'admin'),
                             'dbname':   os.getenv('INFLUX_DB_NA', 'netatmo'),
                             'protocol': 'line'}

    def update_data_today(self):

        # Retrieve data
        data = self._retrieve_data_lastdays(1)

        # Write data to DB
        client = self._get_connection_db()
        self._write_data(client, data)


    def update_data_complete(self):
        print('Retrieve complete {} history...'.format(self.dataname))

        # Retrieve data
        start = '2018-12-01 00:00:00'
        data  = self._retrieve_data_period(start)

        # Write data to DB
        client = self._get_connection_db()
        for data_para in data:
            self._write_data(client, data_para)


    # def _retrieve_data_lastdays(self, days):
    #     print('Retrieve {} data for last {} days'.format(self.dataname, days))
    #
    #     now = datetime.now().timestamp()
    #     ts_start = int(now-days*24*3600)
    #     ts_end   = int(now+3600)
    #
    #     data = self._retrieve_data(self.station_id, 'PM10', ts_start, ts_end)
    #     return data


    def _retrieve_data_period(self, dtg_start):

        data_dir = self.local_file_data

        # delete files in folder
        files = os.listdir(data_dir)
        # if len(files) != 0:
        #     import pdb; pdb.set_trace()

        # Fetch files
        subprocess.run(['/usr/src/app/netatmo.sh', '-s', dtg_start])
        # import pdb; pdb.set_trace()

        # Ensure files are present
        files = os.listdir(data_dir)
        # if len(files) != 7:
        #     import pdb; pdb.set_trace()

        # Read files
        data_all = []
        for file in files:

            station, sensor, *_ = file.split('_')
            data = pd.read_csv(data_dir + file, sep=';', header=2)
            data = data[['Timestamp', sensor]]
            data = data.rename(columns={'Timestamp': 'Time', sensor: '_'.join([station, sensor])})
            data = data.set_index(pd.to_datetime(data.Time, unit='s')).drop('Time', 1)
            data_all.append(data)

        return data_all


    # def _retrieve_data(self, station, parameter, ts_start, ts_end):
    #     param_dict = {'station[]': station,
    #                   'pollutant[]': parameter,
    #                   'scope[]': '1SMW',
    #                   'group[]': 'station',
    #                   'range[]': f'{ts_start},{ts_end}',
    #                   # 'network[]': 'HH',
    #                  }
    #     param ='&'.join([x + '=' + y for x,y in param_dict.items()])
    #     url_param = f'{self.remote_file_data}?{param}'
    #
    #     data = pd.read_csv(url_param, encoding = "ISO-8859-1", sep=';')
    #     data = data.rename(columns={'Zeit': 'Time', 'Messwert (in µg/m³)': 'PM10'})
    #
    #     data.index = pd.to_datetime(data.Time, format='%d.%m.%Y %H:%M')
    #     data = data[['PM10']]
    #     return data

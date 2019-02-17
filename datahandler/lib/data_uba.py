import os
import re
import time
import requests
import pandas as pd
from datetime import datetime
from influxdb import DataFrameClient, InfluxDBClient
from bs4 import BeautifulSoup

from lib.data import Data


class UBA(Data):

    def __init__(self):
        Data.__init__(self)


    def set_config(self):
        # Config

        self.remote_file_data = 'https://www.umweltbundesamt.de/uaq/csv/stations/data'
        self.station_id       = os.getenv('UBA_SENSOR_ID')

        # # Stations:      DEHH026, DEBE065
        # # Parameters:    PM10, NO2, O3, CO, SO2
        # # Scope:         1SMW, 1SMW_MAX, 1TMW
        # # Group:         station, pollutant

        self.influxdb_cfg = {'host':     os.getenv('INFLUX_HOST', 'localhost'),
                             'port':     8086,
                             'user':     os.getenv('INFLUX_USER', 'admin'),
                             'password': os.getenv('INFLUX_PASSWORD', 'admin'),
                             'dbname':   os.getenv('INFLUX_DB_UBA', 'uba'),
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
        start = '2015-01-01T00:00:00'
        end   = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        data  = self._retrieve_data_period(start, end)

        # Write data to DB
        client = self._get_connection_db()
        self._write_data(client, data)


    def _retrieve_data_lastdays(self, days):
        print('Retrieve {} data for last {} days'.format(self.dataname, days))

        now = datetime.now().timestamp()
        ts_start = int(now-days*24*3600)
        ts_end   = int(now+3600)

        data = self._retrieve_data(self.station_id, 'PM10', ts_start, ts_end)
        return data


    def _retrieve_data_period(self, dtg_start, dtg_end):
        ts_start = int(datetime.strptime(dtg_start, '%Y-%m-%dT%H:%M:%S').timestamp())
        ts_end = int(datetime.strptime(dtg_end, '%Y-%m-%dT%H:%M:%S').timestamp()+3600)

        data = self._retrieve_data(self.station_id, 'PM10', ts_start, ts_end)
        return data


    def _retrieve_data(self, station, parameter, ts_start, ts_end):
        param_dict = {'station[]': station,
                      'pollutant[]': parameter,
                      'scope[]': '1SMW',
                      'group[]': 'station',
                      'range[]': f'{ts_start},{ts_end}',
                      # 'network[]': 'HH',
                     }
        param ='&'.join([x + '=' + y for x,y in param_dict.items()])
        url_param = f'{self.remote_file_data}?{param}'

        data = pd.read_csv(url_param, encoding = "ISO-8859-1", sep=';')
        data = data.rename(columns={'Zeit': 'Time', 'Messwert (in µg/m³)': 'PM10'})

        data.index = pd.to_datetime(data.Time, format='%d.%m.%Y %H:%M')
        data = data[['PM10']]
        return data

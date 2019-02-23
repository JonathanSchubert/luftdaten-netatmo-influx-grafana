import os
import re
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from influxdb import DataFrameClient, InfluxDBClient
from bs4 import BeautifulSoup
import json


from lib.data import Data


class UBA(Data):

    def __init__(self):
        Data.__init__(self)

    def set_config(self):
        # Config

        self.remote_file_data    = 'https://www.umweltbundesamt.de/uaq/csv/stations/data'
        self.station_id          = os.getenv('UBA_SENSOR_ID')
        self.local_file_data     = './data/uba_{}.csv'
        self.sensor_file         = './data/sensors_uba.json'

        self.history_start       = '2014-01-01T00:00:00'
        self.hours_update_buffer = 3
        self.update_interval_min = 60 * 55

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

    def _retrieve_data_period(self, dtg_start, dtg_end):
        ts_start = dtg_start.timestamp()
        ts_end = dtg_end.timestamp()+3600

        data = self._retrieve_data(self.station_id, ts_start, ts_end)
        return data

    def _retrieve_data(self, station, ts_start, ts_end):
        sensor = 'PM10'
        param_dict = {'station[]': station,
                      'pollutant[]': sensor,
                      'scope[]': '1SMW',
                      'group[]': 'station',
                      'range[]': f'{ts_start},{ts_end}',
                      # 'network[]': 'HH',
                     }
        param ='&'.join([x + '=' + y for x,y in param_dict.items()])
        url_param = f'{self.remote_file_data}?{param}'

        data = pd.read_csv(url_param, encoding = "ISO-8859-1", sep=';')
        data = data.rename(columns={'Zeit': 'Time', 'Messwert (in µg/m³)': sensor})

        data.index = pd.to_datetime(data.Time, format='%d.%m.%Y %H:%M')
        data = data[[sensor]]
        return {sensor: data}

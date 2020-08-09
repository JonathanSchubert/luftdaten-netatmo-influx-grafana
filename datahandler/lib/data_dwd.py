import os
import re
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from influxdb import DataFrameClient, InfluxDBClient
from bs4 import BeautifulSoup
import json
import numpy as np
from io import BytesIO
from zipfile import ZipFile

from lib.data import Data


class DWD(Data):

    def __init__(self):
        Data.__init__(self)

    def set_config(self):
        # Config

        # Tempelhof:
        #
        # 10min aktualisierte Werte von heute:
        # https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/now/10minutenwerte_TU_00433_now.zip
        #
        # Täglich aktuelisiert Werte bis Gestern, letzte ca 1.5 Jahre
        # https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/recent/10minutenwerte_TU_00433_akt.zip


        self.remote_data         = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/'
        self.station_id          = os.getenv('DWD_STATION_ID')
        self.local_file_data     = './data/dwd_{}.csv'
        self.sensor_file         = './data/sensors_dwd.json'

        self.history_start       = '2019-01-01T00:00:00'
        self.hours_update_buffer = 3
        self.update_interval_min = 60 * 55


        self.influxdb_cfg = {'host':     os.getenv('INFLUX_HOST', 'localhost'),
                             'port':     8086,
                             'user':     os.getenv('INFLUX_USER', 'admin'),
                             'password': os.getenv('INFLUX_PASSWORD', 'admin'),
                             'dbname':   os.getenv('INFLUX_DB_DWD', 'dwd'),
                             'protocol': 'line'}

    def _retrieve_data_period(self, dtg_start, dtg_end):
        ts_start = dtg_start.timestamp()
        ts_end = dtg_end.timestamp() + 3600
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

        file = 'air_temperature/now/10minutenwerte_TU_00433_now.zip'
        data = self._retrieve_data_file_zip(file)

        if dtg_start < today:
            file = 'air_temperature/recent/10minutenwerte_TU_00433_akt.zip'
            data_tmp = self._retrieve_data_file_zip(file)
            data = pd.concat([data_tmp, data]).sort_index()

        data = data[dtg_start:dtg_end]
        data = data.replace(-999, np.NaN)

        data_sep = {}
        for sensor in data.columns:
            data_sep[f'00433_{sensor}'] = data[[sensor]].dropna()

        return data_sep

    def _retrieve_data_file_zip(self, file):
        print('   Retrieve zip file {}'.format(file))

        remote_url = self.remote_data  + file
        resp = requests.get(remote_url).content
        zipfile = ZipFile(BytesIO(resp))
        csvfiles = zipfile.namelist()

        data = []
        for csvfile in csvfiles:
            print('      ... {}'.format(csvfile))

            this_data = pd.read_csv(zipfile.open(csvfile), sep=';')
            this_data.index = pd.to_datetime(this_data.MESS_DATUM, format='%Y%m%d%H%M')
            this_data = this_data[['PP_10', 'TT_10', 'RF_10']]
            this_data.index.name = 'Time'

            data.append(this_data)
        data = pd.concat(data)

        return data

from abc import abstractmethod
from abc import ABC
import os
import json
import pandas as pd
from datetime import datetime, timedelta
import requests
import time
from influxdb import DataFrameClient
import numpy as np


class Data(ABC):

    def __init__(self):
        self.set_config()
        self.client     = None
        self.dataname   = self.__class__.__name__

        print(f'Created {self.dataname}()...')

    @abstractmethod
    def set_config(self):
        pass

    @abstractmethod
    def _retrieve_data_period(self):
        pass

    def create_history(self):
        print(f'Create complete {self.dataname} history...')

        if os.path.exists(self.sensor_file):
            print('   ...history already exists!')
            return

        start = datetime.strptime(self.history_start, '%Y-%m-%dT%H:%M:%S')
        end   = datetime.now() - timedelta(hours=5)

        # Get data
        data  = self._retrieve_data_period(start, end)

        # Write data to local file and DB
        client = self._get_connection_db()
        for sensor, data_sensor in data.items():
            self._write_local_file(data_sensor, sensor)
            self._write_data_db(client, data_sensor)

        self.write_sensor_file(data_sensor.sort_index().index[-1], list(data.keys()))
        print('   ...done')

    def update_latest_data(self):
        print(f'Update latest {self.dataname} data...')

        # Get period to update
        sensor_dict = self.read_sensor_file()
        buffer_time = timedelta(hours=self.hours_update_buffer)
        last_ts = datetime.fromtimestamp(sensor_dict['last_ts']) - buffer_time
        end     = datetime.now() + buffer_time

        # Get data
        print(last_ts, end)
        data  = self._retrieve_data_period(last_ts, end)

        # Write data to local file and DB
        client = self._get_connection_db()
        for sensor, data_sensor in data.items():
            self._update_local_file(data_sensor, sensor)
            self._write_data_db(client, data_sensor)

        self.write_sensor_file(data_sensor.sort_index().index[-1], list(data.keys()))

    def write_complete_history_to_db(self):
        print(f'Write complete {self.dataname} history to DB...')

        # Get sensors
        sensor_dict = self.read_sensor_file()

        client = self._get_connection_db()
        for sensor in sensor_dict['sensors'][::-1]:
            print(f'   {sensor}...')
            data_sensor = self._read_local_file(sensor)

            n_splits = int(np.ceil(data_sensor.shape[0] / 50000))
            print(f'   ...Import splitted into {n_splits} parts')
            data_sensor_splits = np.array_split(data_sensor, n_splits)
            for data_sensor_split in data_sensor_splits:
                self._write_data_db(client, data_sensor_split)
                time.sleep(1)

    def _get_connection_db(self):

        if self.client is not None:
            return self.client

        print('Connect to InfluxDB...')
        db_cfg          = self.influxdb_cfg
        dbname          = db_cfg['dbname']
        influx_conn_ok  = False
        while not influx_conn_ok:
            client = DataFrameClient(db_cfg['host'],
                                     db_cfg['port'],
                                     db_cfg['user'],
                                     db_cfg['password'],
                                     dbname)
            try:
                dbs = client.get_list_database()
                dbs = [x['name'] for x in dbs]
            except requests.exceptions.ConnectionError:
                print('   ...no InfluxDB connection yet. Waiting 5 seconds and retrying.')
                time.sleep(5)
            else:
                print('   ... ok!')
                influx_conn_ok = True

        self.client = client
        return self.client

    def _read_data_db(self, client):
        resp = client.query("select * from {}".format(self.influxdb_cfg['dbname']))
        print(resp)

    def _write_data_db(self, client, data):
        dbname = self.influxdb_cfg['dbname']
        n_entries = data.shape[0]
        self._create_table_if_needed(client, dbname)
        conn_ok  = False

        while not conn_ok:
            try:
                if dbname == 'uba':
                    data = data.astype(int)
                client.write_points(data,
                                    self.influxdb_cfg['dbname'],
                                    protocol=self.influxdb_cfg['protocol'])
            except requests.exceptions.ConnectionError:
                print('   ...no InfluxDB connection yet. Waiting 5 seconds and retrying.')
                time.sleep(5)
                client = self._get_connection_db()
            else:
                print('   ...ok!')
                conn_ok = True
        print('   ...imported {} entries into table {}'.format(n_entries, dbname))

    def _create_table_if_needed(self, client, tablename):
        dbs = client.get_list_database()
        dbs = [x['name'] for x in dbs]
        if tablename not in dbs:
            print("Create database: " + tablename)
            client.create_database(tablename)

    def _make_dir_avalable(self, dir):
        """
        Create directorie handed over, if not yet available
        """
        if not os.path.exists(dir):
            os.makedirs(dir)
            print('created following dir: {}'.format(dir))

    def write_sensor_file(self, last_ts, sensors):
        sensor_dict = {'sensors': sensors,
                       'last_ts': last_ts.timestamp(),
                       'last_ts_fmt': last_ts.strftime('%Y-%m-%dT%H:%M:%S')}
        with open(self.sensor_file, 'w') as fp:
            json.dump(sensor_dict, fp)

    def read_sensor_file(self):
        with open(self.sensor_file, 'r') as fp:
            sensor_dict = json.load(fp)
        return sensor_dict

    def _write_local_file(self, data, sensor):
        data.to_csv(self.local_file_data.format(sensor))

    def _read_local_file(self, sensor):
        data = pd.read_csv(self.local_file_data.format(sensor), index_col='Time', parse_dates=['Time'])
        return data

    def _update_local_file(self, data, sensor):
        data_file = self._read_local_file(sensor)

        len_before = data_file.shape[0]
        data = data.reset_index()
        data_file = data_file.reset_index()
        data_updated = data_file.merge(data, how='outer').set_index('Time').sort_index()

        self._write_local_file(data_updated, sensor)
        print('   ...added {} entries to dataset'.format(data_updated.shape[0] - len_before))

    # def _get_period_in_db(self, client):
    #     dbname = self.influxdb_cfg['dbname']
    #     res = client.query("select * from {}".format(dbname))[dbname].sort_index()
    #
    #     days = set([x.strftime('%Y-%m-%d') for x in res.index.tolist()])
    #
    #     return list(days)

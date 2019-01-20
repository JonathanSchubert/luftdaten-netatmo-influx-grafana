import pandas as pd
import time
import os

from influxdb import DataFrameClient, InfluxDBClient
from bs4 import BeautifulSoup
import requests
import re


def main():

    # Config
    remote_file_data = 'https://www.madavi.de/sensor/data_csv/data-{}-{}.csv'
    remote_file_list = 'https://www.madavi.de/sensor/csvfiles.php?sensor={}'
    station_id = os.getenv('LD_SENSOR_ID')

    influxdb_cfg = {'host':     os.getenv('INFLUX_HOST', 'localhost'),
                    'port':     8086,
                    'user':     os.getenv('INFLUX_USER', 'admin'),
                    'password': os.getenv('INFLUX_PASSWORD', 'admin'),
                    'dbname':   os.getenv('INFLUX_DB', 'luftdaten'),
                    'protocol': 'json'}

    sensors = {'Temperature': 'Temp',
               'Humidity':    'Humidity',
               'PM2.5':       'SDS_P1',
               'PM10':        'SDS_P2'}

    hist_created = False
    while True:
        # Get connection and create database
        client = get_connection_db(influxdb_cfg)

        # Get period of available data
        days = get_period_data_avail(remote_file_list, station_id)

        if hist_created:
            days = [days[-1]]
        else:
            hist_created = True

        for day in days:
            # Retrieve data
            data = retrieve_data_day(day, remote_file_data, station_id)
            # print(data.tail())

            # Write data to DB
            client.write_points(data, influxdb_cfg['dbname'], protocol=influxdb_cfg['protocol'])

        # print("Read DataFrame")
        # resp = client.query("select * from {}".format(influxdb_cfg['dbname']))
        # print(resp)

        # print("Delete database: " + dbname)
        # client.drop_database(dbname)

        time.sleep(55)

def get_period_data_avail(remote_file_list, station_id):
    print('Get period of available data...')
    resp = requests.get(remote_file_list.format(station_id))

    if resp.status_code == 200:
        soup = BeautifulSoup(resp.text, "lxml")
        links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if 'data' in href:
                links.append(href)
    else:
        exit(1)

    dates_avail = sorted([re.findall("(\d{4}-\d{2}-\d{2})", x)[0] for x in links])
    print('    ... found', dates_avail[0], dates_avail[-1])
    return sorted(dates_avail)

def get_connection_db(influxdb_cfg):

    dbname = influxdb_cfg['dbname']

    influx_conn_ok = False
    print('Connect to InfluxDB...')

    while not influx_conn_ok:
        print('     ', influxdb_cfg['host'], influxdb_cfg['port'], influxdb_cfg['user'], influxdb_cfg['password'], dbname)
        client = DataFrameClient(influxdb_cfg['host'],
                                 influxdb_cfg['port'],
                                 influxdb_cfg['user'],
                                 influxdb_cfg['password'],
                                 dbname)
        try:
            dbs = client.get_list_database()
            dbs = [x['name'] for x in dbs]
        except requests.exceptions.ConnectionError:
            print('   ...no InfluxDB connection yet. Waiting 5 seconds and retrying.')
            time.sleep(5)
        else:
            influx_conn_ok = True

    if dbname not in dbs:
        print("Create database: " + dbname)
        client.create_database(dbname)

    return client

def retrieve_data_day(day, remote_file_data, station_id):

    print('Retrieve data for day {}'.format(day))
    remote_url = remote_file_data.format(station_id, day)
    data = pd.read_csv(remote_url, sep=';')
    data = data.set_index(pd.to_datetime(data.Time))

    # usefull_columns = ['Time', 'SDS_P1', 'SDS_P2', 'Temp', 'Humidity', 'Samples', 'Min_cycle', 'Max_cycle', 'Signal']
    # data_sel = data[usefull_columns]

    data_sel = data[['SDS_P1', 'SDS_P2', 'Temp', 'Humidity']].dropna()

    return data_sel

main()

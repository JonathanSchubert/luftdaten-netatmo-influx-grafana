import os
import pandas as pd
import subprocess
import dateutil.rrule as rrule

from lib.data import Data


class Netatmo(Data):

    def __init__(self):
        Data.__init__(self)

    def set_config(self):
        # Config
        self.local_file_tmp_data = './data/netatmo_tmp_data/'
        self.local_file_data     = './data/netatmo_{}.csv'
        self.sensor_file         = './data/sensors_netatmo.json'

        self.history_start       = '2016-04-01T00:00:00'
        self.hours_update_buffer = 3
        self.update_interval_min = 60 * 5

        self.influxdb_cfg = {'host':     os.getenv('INFLUX_HOST', 'localhost'),
                             'port':     8086,
                             'user':     os.getenv('INFLUX_USER', 'admin'),
                             'password': os.getenv('INFLUX_PASSWORD', 'admin'),
                             'dbname':   os.getenv('INFLUX_DB_NA', 'netatmo'),
                             'protocol': 'line'}

    def _retrieve_data_period(self, dtg_start, dtg_end):

        tmp_dir = self.local_file_tmp_data
        self._make_dir_avalable(tmp_dir)

        # delete files in folder
        files = os.listdir(tmp_dir)
        if len(files) != 0:
            [os.remove(tmp_dir + x) for x in files]

        # Fetch files
        for i, this_period in enumerate(self.periods_aligned(dtg_start, dtg_end)):
            if i == 0:
                last_period = this_period
                continue
            dtg_end_this   = this_period.strftime('%Y-%m-%d %H:%M:%S')
            dtg_start_this = last_period.strftime('%Y-%m-%d %H:%M:%S')
            last_period = this_period
            print(dtg_start_this, dtg_end_this)
            command = ['/usr/src/app/netatmo.sh', '-s', dtg_start_this, '-e', dtg_end_this, '-o', tmp_dir]
            print(command)
            subprocess.run(command)

        # Ensure files are present
        files = os.listdir(tmp_dir)
        # if len(files) != 7:
        #     import pdb; pdb.set_trace()

        # Read files
        data_all = {}
        for file in files:
            station, sensor, *_ = file.split('_')
            this_id = '_'.join([station, sensor])

            data = pd.read_csv(tmp_dir + file, sep=';', header=2)
            data = data[['Timestamp', sensor]]
            data = data.rename(columns={'Timestamp': 'Time', sensor: this_id})
            data = data.set_index(pd.to_datetime(data.Time, unit='s')).drop('Time', 1)

            if this_id in data_all.keys():
                data_all[this_id].append(data)
            else:
                data_all[this_id] = [data]

        data_all2 = {}
        for this_id in data_all.keys():
            data_all2[this_id] = pd.concat(data_all[this_id]).sort_index()

        return data_all2

    def periods_aligned(self, start, end, inc=True):
        if inc:
            yield start
        rule = rrule.rrule(rrule.YEARLY, byminute=0, bysecond=0, dtstart=start)
        for x in rule.between(start, end, inc=False):
            yield x
        if inc:
            yield end

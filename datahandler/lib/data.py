from abc import abstractmethod
from abc import ABC

from influxdb import DataFrameClient, InfluxDBClient

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
    def update_data_complete(self):
        pass

    @abstractmethod
    def update_data_today(self):
        pass

    def _get_period_in_db(self, client):
        dbname = self.influxdb_cfg['dbname']
        res = client.query("select * from {}".format(dbname))[dbname].sort_index()

        days = set([x.strftime('%Y-%m-%d') for x in res.index.tolist()])

        return list(days)

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

    def _read_data(self, client):
        resp = client.query("select * from {}".format(self.influxdb_cfg['dbname']))
        print(resp)

    def _write_data(self, client, data):
        self._create_table_if_needed(client, self.influxdb_cfg['dbname'])
        client.write_points(data,
                            self.influxdb_cfg['dbname'],
                            protocol=self.influxdb_cfg['protocol'])

    def _create_table_if_needed(self, client, tablename):
        dbs = client.get_list_database()
        dbs = [x['name'] for x in dbs]
        if tablename not in dbs:
            print("Create database: " + tablename)
            client.create_database(tablename)

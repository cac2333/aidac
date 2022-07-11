import unittest

import aidac


class TestDataFrameBase(unittest.TestCase):
    def setUp(self) -> None:
        from tests.ds_config import PG_CONFIG
        config = PG_CONFIG
        aidac.add_data_source('postgres', config['host'], config['user'], config['passwd'], config['dbname'], 'p1', config['port'])
        self.station = aidac.read_remote_data('p1', 'station')
        self.trip = aidac.read_remote_data('p1', 'tripdata2017')

        self.trip_path = 'resources/tripdata2017.csv'
        self.station_path = 'resources/stations2017.csv'

if __name__ == '__main__':
    unittest.main()
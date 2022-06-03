import unittest
import aidac

import numpy as np

class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        from tests.ds_config import PG_CONFIG
        config = PG_CONFIG
        aidac.add_data_source('postgres', config['host'], config['user'], config['passwd'], config['dbname'], 'p1', config['port'])
        self.station = aidac.read_remote_data('p1', 'station')
        # self.users = aidac.read_remote_data('p1', 'users')
        # self.review = aidac.read_remote_data('p1', 'review')

    def test_schedule_transfer(self):
        self.trip = aidac.from_dict({'id': np.asarray([1, 7060, 6203, 6001, 6002]), 'duration': np.asarray([100, 20, 60, 50, 38])})
        proj = self.station[['id', 'name']]
        jn = proj.merge(self.trip, 'id', 'inner')
        jn.materialize()
        print(jn.data)


if __name__ == '__main__':
    unittest.main()

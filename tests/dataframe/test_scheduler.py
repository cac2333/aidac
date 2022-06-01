import unittest
import aidac


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        from tests.ds_config import PG_CONFIG
        config = PG_CONFIG
        aidac.add_data_source('postgres', config['host'], config['user'], config['passwd'], config['dbname'], 'p1', config['port'])
        self.station = aidac.read_remote_data('p1', 'station')
        self.users = aidac.read_remote_data('p1', 'users')
        self.review = aidac.read_remote_data('p1', 'review')

    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()

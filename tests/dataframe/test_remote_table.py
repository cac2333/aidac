import unittest
import re

import aidac
from aidac.dataframe.transforms import SQLProjectionTransform


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        from tests.ds_config import PG_CONFIG
        config = PG_CONFIG
        aidac.add_data_source('postgres', config['host'], config['user'], config['passwd'], config['dbname'], 'p1', config['port'])
        self.station = aidac.read_remote_data('p1', 'station')

    def test_remote_project1(self):
        proj1 = self.station[['id', 'name', 'longti']]
        self.assertTrue(isinstance(proj1.transform, SQLProjectionTransform))
        sql1 = proj1.transform.genSQL
        self.assertEqual(sql1, 'SELECT id AS id, name AS name, longti AS longti FROM (SELECT * FROM station) station')

        proj2 = proj1['longti']
        sql2 = proj2.transform.genSQL
        expected = 'SELECT longti AS longti FROM ({})'.format(sql1)
        self.assertRegex(sql2, re.escape(expected))

    def test_schdule1(self):
        proj = self.station['id']
        proj.materialize()


if __name__ == '__main__':
    unittest.main()

import datetime
import unittest
from unittest.mock import Mock

import numpy as np

import aidac
from aidac.common.column import Column
from aidac.dataframe.frame import DataFrame
from aidac.dataframe.frame_wrapper import WFrame


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        path = 'resources/dummy_table.csv'
        self.local_table = aidac.read_csv(path, header=0)

        tb = WFrame(DataFrame(table_name='my_remote', ds=Mock()))
        tb._tail_frame._columns_ = {
            'int_col': Column(name='int_col', dtype=int, table='my_remote', source_table='my_remote'),
            'float_col': Column(name='float_col', dtype=np.float64, table='my_remote', source_table='my_remote'),
            'date_col': Column(name='date_col', dtype=datetime.date, table='my_remote', source_table='my_remote'),
            'str_col': Column(name='str_col', dtype=object, table='my_remote', source_table='my_remote')
        }
        self.remote_table = tb

    def test_column(self):
        cols = self.local_table.columns
        self.assertIn('id', cols)
        self.assertIn('name', cols)
        self.assertIn('salary', cols)

        self.assertEqual(cols.get('id').dtype, np.int64)
        self.assertEqual(cols.get('name').dtype, np.object)
        self.assertEqual(cols.get('salary').dtype, np.float64)

    def test_project_table(self):
        proj = self.remote_table
        proj['new_int'] = 1
        tb = proj._tail_frame
        self.assertCountEqual(tb.columns.keys(), ['int_col', 'float_col', 'str_col', 'date_col', 'new_int'])
        self.assertEqual(tb.columns['new_int'], Column(name='new_int', dtype=int, expr='1'))
        self.assertEqual(tb._saved_args_, ['new_int', 1])


if __name__ == '__main__':
    unittest.main()

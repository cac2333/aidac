import unittest

from tests.dataframe.test_dataframe_base import TestDataFrameBase
from aidac.exec.Executable import *

class MyTestCase(TestDataFrameBase):
    def test_hist(self):
        hist1 = get_hist(self.station, 'id')
        self.station.materialize()
        hist2 = get_hist(self.station, 'id')
        self.assertEqual(hist1.n_distinct, hist2.n_distinct)
        self.assertEqual(hist1.null_frac, hist2.null_frac)


if __name__ == '__main__':
    unittest.main()

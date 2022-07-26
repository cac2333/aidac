import time
import unittest
from unittest.mock import Mock

import aidac

import numpy as np

from aidac.dataframe.frame import RemoteTable
from tests.dataframe.test_dataframe_base import TestDataFrameBase
from aidac.exec.Executable import *


class MyTestCase(TestDataFrameBase):
    def test_schedule_transfer1(self):
        self.trip = aidac.from_dict({'id': np.asarray([1, 7060, 6203, 6001, 6002]), 'duration': np.asarray([100, 20, 60, 50, 38])})
        proj = self.station[['scode', 'sname']]
        jn = self.trip.merge(proj, left_on='id', right_on='scode', how='inner')
        jn.materialize()
        print(jn.data)

    def test_schedule_transfer2(self):
        trip = aidac.read_csv(self.trip_path, names=['tid', 'starttm', 'stscode', 'endtm', 'endsode', 'duration', 'is_member'])
        start = time.time()
        jn = self.station.merge(trip, left_on='scode', right_on='stscode')
        proj = jn[['scode', 'sname', 'duration']]
        proj.materialize()
        end = time.time()
        print(end-start)
        print(proj)

    def test_schedule_transfer3(self):
        station = aidac.read_csv(self.station_path, names=['scode', 'sname', 'lati', 'longi', 'sispublic'])
        start = time.time()
        jn = station.merge(self.trip, left_on='scode', right_on='stscode')
        proj = jn[['scode', 'sname', 'duration']]
        proj.materialize()
        end = time.time()
        print(end-start)
        print(proj.data)

    def test_schedule_local_pd(self):
        proj = self.station['scode']
        si = self.station.sort_index()
        print('')


if __name__ == '__main__':
    unittest.main()

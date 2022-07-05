import time
import unittest
import aidac

import numpy as np

from tests.dataframe.test_dataframe_base import TestDataFrameBase


class MyTestCase(TestDataFrameBase):
    def test_schedule_transfer1(self):
        self.trip = aidac.from_dict({'id': np.asarray([1, 7060, 6203, 6001, 6002]), 'duration': np.asarray([100, 20, 60, 50, 38])})
        proj = self.station[['id', 'name']]
        jn = self.trip.merge(proj, 'id', 'inner')
        jn.materialize()
        print(jn.data)

    def test_schedule_transfer2(self):
        trip = aidac.read_csv(self.trip_path, names=['tid', 'starttm', 'stscode', 'endtm', 'endsode', 'duration', 'is_member'])
        start = time.time()
        jn = self.station.merge(trip, left_on=['id'], right_on=['stscode'])
        proj = jn[['id', 'name', 'duration']]
        proj.materialize()
        end = time.time()
        print(end-start)
        print(proj)

    def test_schedule_transfer3(self):
        station = aidac.read_csv(self.station_path, names=['id', 'name', 'lati', 'longi', 'sispublic'])
        start = time.time()
        jn = station.merge(self.trip, left_on=['id'], right_on=['stscode'])
        proj = jn[['id', 'name', 'duration']]
        proj.materialize()
        end = time.time()
        print(end-start)
        print(proj.data)


if __name__ == '__main__':
    unittest.main()

import unittest
from unittest.mock import Mock

import numpy as np

import aidac

from aidac.dataframe.frame import RemoteTable
from aidac.exec.Executable import *


class MyTestCase(unittest.TestCase):

    def mock_ds(self):
        ds_mock = Mock()
        row_num = 100
        hist = ('mock table', .1, .95, 2)
        ds_mock.row_count.return_value = row_num
        ds_mock.get_hist.return_value = hist
        ds_mock.job_name = 'mock job'
        return ds_mock

    def test_executable_plan(self):
        """
        pg
        |
        proj    local
        |           |
        ____join_____
            |
            proj
        @return:
        """
        ds = self.mock_ds()
        station = RemoteTable(source=ds, table_name='mock station')
        station._columns_ = {'id': Column('id', int, 'mock station', 'mock station', False),
                             'name': Column('name', object, 'mock station', 'mock station', False)}
        proj = station[['id', 'name']]
        trip = aidac.from_dict(
            {'id': np.asarray([1, 7060, 6203, 6001, 6002]), 'duration': np.asarray([100, 20, 60, 50, 38])})

        proj_exec = Executable(proj)
        local_exec = Executable(trip)

        jn = proj.merge(trip, 'id', 'inner')
        final_rs = jn['id']

        join_sc = ScheduleExecutable(jn)
        join_sc.add_prereq(proj_exec, local_exec)
        final_exec = Executable(final_rs)
        final_exec.add_prereq(join_sc)
        result_plan = final_exec.plan()
        self.assertEqual(len(result_plan), 2)

        root = RootExecutable()
        root.add_prereq(final_exec)
        root.plan()

        test_sc = final_exec.prereqs[0]
        self.assertTrue(isinstance(test_sc, ScheduleExecutable))
        self.assertEqual(len(test_sc.prereqs), 2)
        self.assertTrue(isinstance(test_sc.prereqs[0], TransferExecutable))
        self.assertTrue(test_sc.prereqs[0].prereqs[0].__class__ == Executable)



if __name__ == '__main__':
    unittest.main()

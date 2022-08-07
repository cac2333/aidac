import unittest
from unittest.mock import Mock

import numpy as np

import aidac

from aidac.dataframe.frame import DataFrame
from aidac.exec.Executable import *


class MyTestCase(unittest.TestCase):

    def mock_ds(self, rows=100, width=512, hist=('mock table', 0.01, .6, 0)):
        ds_mock = Mock()
        ds_mock.row_count.return_value = rows
        ds_mock.get_hist.return_value = hist
        ds_mock.get_estimation.return_value = (rows, width)
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
        ds = self.mock_ds(100, 40, ('mock table', 0, 2, 0))
        station = DataFrame(ds=ds, table_name='mock station')
        station._columns_ = {'id': Column('id', int, 'mock station', 'mock station', False),
                             'name': Column('name', object, 'mock station', 'mock station', False)}
        proj = station[['id', 'name']]
        trip = aidac.from_dict(
            {'id': np.asarray([1, 7060, 6203, 6001, 6002, 8080, 1000]), 'duration': np.asarray([100, 20, 60, 50, 38, 60, 55])})

        proj_exec = Executable(proj)
        local_exec = Executable(trip)

        jn = proj.merge(trip, 'id', 'inner')
        final_rs = jn['id']

        final_exec = Executable(final_rs)

        join_sc = ScheduleExecutable(jn, final_exec)
        join_sc.add_prereq(proj_exec, local_exec)

        final_exec.add_prereq(join_sc)

        root = RootExecutable()
        root.add_prereq(final_exec)
        result_plan = root.plan()
        self.assertEqual(len(result_plan), 2)

        self.assertEqual(final_exec.planned_job, 'mock job')
        test_sc = final_exec.prereqs[0]
        self.assertTrue(isinstance(test_sc, ScheduleExecutable))
        self.assertEqual(len(test_sc.prereqs), 2)
        self.assertTrue(isinstance(test_sc.prereqs[0], TransferExecutable))
        self.assertTrue(test_sc.prereqs[1].__class__ == Executable)
        self.assertEqual(test_sc.prereqs[1].planned_job, 'mock job')
        self.assertTrue(test_sc.prereqs[0].prereqs[0].__class__ == Executable)



if __name__ == '__main__':
    unittest.main()

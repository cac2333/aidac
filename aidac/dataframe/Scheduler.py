from __future__ import annotations

import weakref
from collections.abc import Iterable

from aidac.data_source.DataSourceManager import manager
from aidac.common.meta import MetaInfo
import aidac.dataframe.frame as frame
from aidac.exec.Executable import *

LOCAL_DS = '_local'


def is_local(df1: frame.DataFrame, df2: frame.DataFrame):
    """
    check if two dataframes are local to each other
    @param df1:
    @param df2:
    @return:
    """
    if df1.data_source is not None and df2.data_source is not None:
        # if two data frames have the same source then they are considered local to each other
        if df1.data_source.job_name == df2.data_source.job_name:
            return True
        # if there are other data sources / stubs associated with the df, we check for all stubs
        if df1._stubs_:
            for stub in df1._stubs_:
                if is_local(stub, df2):
                    return True
        elif df2._stubs_:
            for stub in df2._stubs_:
                if is_local(stub, df1):
                    return True
        return False


class Scheduler:
    def __init__(self):
        self.sources = {}
        self.source_manager = manager

    def execute(self, df: frame.DataFrame):
        """
        materialize the lineage, execute util the data to be transfered to another data source
        @param df:
        @return:
        """
        def _gen_pipe(df):
            ex1 = Executable(df)
            stack = [df]
            while stack:
                cur = stack.pop()
                if cur._data_ is not None or cur._db_persistent:
                    return ex1
                else:
                    assert cur.transform is not None
                    sources = cur.transform.sources()
                    from aidac.dataframe.transforms import SQLJoinTransform
                    from aidac.common.aidac_types import is_type
                    from aidac.common.aidac_types import ArrayLike
                    if not is_type(sources, ArrayLike):
                        stack.append(sources)
                    else:
                        sblock = ScheduleExecutable(weakref.proxy(cur), ex1)
                        for s in sources:
                            sblock.add_prereq(_gen_pipe(s))
                        ex1.add_prereq(sblock)
            return ex1
        # self._dfs_link_ds(df)
        ex = _gen_pipe(df)
        root_ex = RootExecutable()
        root_ex.add_prereq(ex)
        my_plan = root_ex.plan()
        print(my_plan)
        return root_ex.process()


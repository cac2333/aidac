from __future__ import annotations

from collections.abc import Iterable

from aidac.data_source.DataSourceManager import manager
from aidac.common.meta import MetaInfo
import aidac.dataframe.frame as frame
from aidac.exec.Executable import *

LOCAL_DS = '_local'


def _link_tb(df: frame.RemoteTable, spc_ds=None) -> str | None:
    """
    check if the corresponding table exists in the remote data source
    @param df:
    @return: None if no matching table is found, otherwise return the name of the table
    """
    ds = spc_ds if spc_ds else df.data_source
    if df.source_table is not None:
        return df.source_table
    else:
        tid = df.id
        if ds is None or tid not in ds.ls_tables():
            return None
        else:
            return tid


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

    def _is_local(self, df: frame.DataFrame, ds: str = None) -> bool:
        """
        Find if a dataframe is local to a datasource
        @param df: Dataframe node to work with
        @param ds: data source name, default is none and will check if the dataframe has a local copy
        @return: boolean
        """
        cur = df
        if ds:
            if isinstance(cur, frame.LocalTable):
                # follow the same procedure as remote db if local table has a remote stub
                if ds in cur._stubs_:
                    cur = cur._stubs_[ds]
                else:
                    return False
            return True if _link_tb(cur) else False
        else:
            return df._data_ is not None

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
                if cur._data_ is not None or cur.source_table is not None:
                    return ex1
                else:
                    assert cur.transform is not None
                    sources = cur.transform.sources()
                    if isinstance(sources, frame.DataFrame):
                        stack.append(sources)
                    else:
                        sblock = ScheduleExecutable(cur)
                        for s in sources:
                            # todo: check if source in the same data source as current
                            sblock.add_prereq(_gen_pipe(s))
                        ex1.add_prereq(sblock)
            return ex1
        # self._dfs_link_ds(df)
        ex = _gen_pipe(df)
        root_ex = RootExecutable()
        root_ex.add_prereq(ex)
        root_ex.plan()
        return root_ex.process()


    def meta(self, df: frame.DataFrame):
        """
        meta information stored in different data sources should all be the same
        here just use one data source to estimate the meta information
        @param df:
        @return:
        """

        if isinstance(df, frame.LocalTable) or df._data_ is not None:
            meta = MetaInfo(df.columns, len(df.columns), len(df._data_))
            return meta

        tb_name = _link_tb(df)
        if tb_name is None:
            assert df.transform is not None, "A table without a remote table linked to it must have a transform"
            # todo: use estimator to decide the meta
            if isinstance(df.transform.sources(), tuple):
                return self.meta(df.transform.sources()[0])
            return self.meta(df.transform.sources())
        else:
            nr = df.data_source.row_count(tb_name)
            meta = MetaInfo(df.columns, len(df.columns), nr)
            return meta

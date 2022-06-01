from __future__ import annotations

from collections.abc import Iterable

from aidac.data_source.DataSourceManager import manager
from aidac.common.meta import MetaInfo
import aidac.dataframe.frame as frame
from aidac.exec.Executable import Executable, TransferExecutable


def _link_tb(df: frame.RemoteTable) -> str | None:
    """
    check if the corresponding table exists in the remote data source
    @param df:
    @return: None if no matching table is found, otherwise return the name of the table
    """
    ds = df.source
    if df.tbl_name is not None:
        return df.tbl_name
    else:
        tid = str(df.__tid__)
        if tid not in ds.ls_tables():
            return None
        else:
            return tid


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
                if ds in cur._stub_:
                    cur = cur._stub_[ds]
                else:
                    return False
            return True if _link_tb(cur) else False
        else:
            return df._data_ is not None

    def _max_card(self, metas):
        """

        @param metas:
        @return: datasource that has most data
        """
        jobs = {}
        for df, mt in metas:
            jname = df.source.job_name
            # use the product of row and column to estimate the cardinality
            if jname in jobs:
                jobs[jname] += mt.ncols * mt.nrows
            else:
                jobs[jname] = mt.ncols * mt.nrows

        maxc = 0
        opt = None
        for jname, card in jobs.items():
            if card > maxc:
                maxc = card
                opt = jname
        # todo: maxc might not be the desired opt meta
        return opt, maxc

    def _dfs_link_ds(self, df: frame.DataFrame) -> (str, MetaInfo):
        """
        Link data source at the same time
        @param df:
        @return: (ds job name, meta)
        """
        if isinstance(df, frame.LocalTable) or df._data_ is not None or df.transform is None:
            return None, self.meta(df)
        else:
            assert df.transform is not None
            src_meta = []
            # if have multiple sources, use the one has the largest cardinality
            for s in df.transform.sources():
                meta = self.meta(s)
                src_meta.append((s, meta))
            opt_ds, opt_meta = self._max_card(src_meta)

            ds = self.source_manager.get_data_source(opt_ds)
            assert ds is not None
            # use parent's datasource
            # todo: duplicated data over different places
            if df.source is None:
                df.source = ds
            else:
                df.add_source(ds)
        return opt_ds, opt_meta

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
                if cur._data_ is not None:
                    return ex1
                else:
                    assert cur.transform is not None
                    sources = cur.transform.sources()
                    for s in sources:
                        # todo: check if source in the same data source as current
                        if _link_tb(s, cur.source):
                            stack.append(s)
                        else:
                            # datasources diverge here, create a data transfer node
                            pex = TransferExecutable(cur)
                            pex.add_prereq(_gen_pipe(s))
                            ex1.add_prereq(pex)
            return ex1
        self._dfs_link_ds(df)
        ex = _gen_pipe(df)
        return ex.process()


    def schedule(self, df: frame.DataFrame):
        """
        Schedule and build execution plan for the given dataframe
        @param df:
        @return:
        """
        if df._data_ is not None:
            return df._data_
        else:
            self._dfs_link_ds(df)
            data = df.source._execute(df.genSQL)
            return data

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
            if isinstance(df.transform.sources(), list):
                return self.meta(df.transform.sources()[0])
            return self.meta(df.transform.sources())
        else:
            nr = df.source.row_count(tb_name)
            meta = MetaInfo(df.columns, len(df.columns), nr)
            return meta

from __future__ import annotations

from aidac.common.meta import MetaInfo
import aidac.dataframe.frame as frame
from aidac.data_source.DataSourceManager import DataSourceManager
from aidac.exec.Executable import Executable


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
        self.source_manager = DataSourceManager()

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
        @return:
        """
        if isinstance(df, frame.LocalTable) or df._data_ is not None or df.transform is not None:
            return None, self.meta(df)
        else:
            assert df.transform is not None
            src_meta = []
            for s in df.transform.sources:
                meta = self._dfs_link_ds(s)
                src_meta.append((s, meta))
            opt_ds, opt_meta = self._max_card(src_meta)

            ds = self.source_manager.get_data_source(opt_ds)
            assert ds is not None
            if df.source is None:
                df.source = ds
            else:
                df.add_source(ds)
        return opt_ds, opt_meta

    def transfer_all(self, df: frame.DataFrame):
        opt_ds,  _ = self._dfs_link_ds(df)

    def transfer(self, src: frame.DataFrame, dest: str):
        """
        Transfer data from one datasource to another
        @param src: source table to be transferred
        @param dest: dataframe whose datasource would be the destination
        @return: local stub points to the temporary table?
        """
        scols = src.columns
        # todo: check for duplicate names
        dest.datasource.create_table(src.table_name, scols)
        dest.datasource.import_table(src.table_name, src.data)
        # todo: decide if a local stub should be created

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

    def retrieve_columns(self, df: frame.RemoteTable):
        # todo: solve multiple ds
        tb_name = _link_tb(df)
        if tb_name is None:
            assert df.transform is not None, "A table without a remote table linked to it must have a transform"
            # materialize column info
            df.transform.column
            return None
        else:
            return df.source.table_meta_data(tb_name)

    def meta(self, df: frame.RemoteTable):
        # todo: solve multiple ds
        tb_name = _link_tb(df)
        if tb_name is None:
            assert df.transform is not None, "A table without a remote table linked to it must have a transform"
            # todo, recursively compute the meta info
            return None
        else:
            nr, nc = df.source.cardinality(tb_name)
            meta = MetaInfo(df.columns, nc, nr)
            return meta

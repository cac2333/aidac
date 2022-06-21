from __future__ import annotations

import pandas as pd

from aidac.common.DataIterator import generator
from aidac.common.column import Column
from aidac.common.hist import Histgram
from aidac.common.meta import MetaInfo
from aidac.dataframe import frame
from aidac.exec.utils import *

from aidac.data_source.DataSourceManager import manager, LOCAL_DS


def is_local(df1: frame.DataFrame, df2: frame.DataFrame):
    """
    check if two dataframes are local to each other
    @param df1:
    @param df2:
    @return:
    """
    if df1.source is not None and df2.source is not None:
        # if two data frames have the same source then they are considered local to each other
        if df1.source.job_name == df2.source.job_name:
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


def get_meta(df: frame.DataFrame):
    if df._data_ is not None:
        meta = MetaInfo(df.columns, len(df.columns), len(df._data_))
        return meta
    else:
        nr = df.source.row_count(df.table_name)
        meta = MetaInfo(df.columns, len(df.columns), nr)
        return meta


def get_hist(df: frame.DataFrame, col: Column):
    if df._data_ is not None:
        n_null = len(df.data[col.name].isnull())
        total = len(df.data)
        n_distinct = len(df.data[col.name].unique())
        hist = Histgram(df.table_name, n_null/total, n_distinct)
    else:
        table_name, null_frac, n_distinct, mcv = df.source.get_hist(df.table_name, col.name)
        hist = Histgram(table_name, null_frac, n_distinct, mcv)
    return hist


class Executable:
    def __init__(self, df):
        self.df = df
        self.prereqs = []
        self.planned_job = None
        self.estimation = None

    def process(self):
        """
        Need to process all prerequisites and update the lineage
        @return:
        """
        if isinstance(self.df, frame.LocalTable):
            return self.df.data

        for x in self.prereqs:
            x.process()

        if self.planned_job == LOCAL_DS:
            ss = self.df.transform.sources()
            data = pd.merge(ss[0].data, ss[1].data, left_on='id', right_on='stscode', how='inner')
        else:
            import time
            print('process')
            start = time.time()
            sql = self.df.genSQL
            print('sql generated')
            print(time.time() - start)
            rs = self.df.source._execute(sql)
            print('returned')
            print(time.time() - start)
            data = rs.get_result_table()
            print(time.time()-start)
            data = pd.DataFrame(data)
        self.clear_lineage()
        self.df._data_ = data
        return data

    def plan(self):
        for x in self.prereqs:
            x.plan()

    def add_prereq(self, other: Executable):
        self.prereqs.append(other)

    def clear_lineage(self):
        self.df.clear_lineage()


class TransferExecutable(Executable):
    def __init__(self, df, prereqs=[], dest=None):
        self.df = df
        self.prereqs = prereqs
        self.dest = dest

    def transfer(self, src: frame.DataFrame, dest: str):
        """
        Transfer data from one datasource to another
        @param src: source table to be transferred
        @param dest: destination data source
        @return: local stub points to the temporary table?
        """
        scols = src.columns
        # todo: check for duplicate names
        if dest != LOCAL_DS:
            dest_ds = manager.get_data_source(dest)
            dest_ds.create_table(src.table_name, scols)
            dest_ds.import_table(src.table_name, scols, generator(src.data))
        # todo: decide if a local stub should be created

    def process(self):
        """
        Need to process all prerequisites and update the lineage
        @return:
        """
        for x in self.prereqs:
            x.process()

        if self.dest is not None:
            self.transfer(self.df, self.dest)

    def plan(self):
        return

    def add_prereq(self, other: Executable):
        self.prereqs.append(other)

    def clear_lineage(self):
        return


class ScheduleExecutable(Executable):
    """
        se
        |
    -----------
    |      |
    ex     ex
    determine the data transfer direction from nodes beneath it
    only make local optimal decision
    """

    def __init__(self, prev):
        self.prev = prev
        self.prereqs = []

    def _traverse2end(self, df: frame.DataFrame):
        """
        traverse to the end of one branch
        @param df:
        @return: all data sources and the estimated meta data of the df
        """
        all_source = set()
        card = None
        for s in df._stubs_:
            new_ds, card = self._traverse2end(s)
            all_source.update(new_ds)
        # local table and database table
        if df.source.job_name == '_local_ds' or df.table_name in df.source.ls_tables():
            all_source.add(df.source)
            card = get_meta(df)
        else:
            # we are sure here we will not have a fork, as it would be captured by a new scheduleExecutable
            assert df.transform is not None
            assert isinstance(df.transform.sources(), frame.DataFrame)
            new_ds, card = self._traverse2end(df.transform.sources())
            all_source.update(new_ds)
        return all_source, card

    def _max_card(self, metas):
        """

        @param metas:
        @return: datasource that has most data
        """
        jobs = {}
        for jname, mt in metas.items():
            # use the product of row and column to estimate the cardinality
            jobs[jname] = sum(map(lambda x: x.ncols*x.nrows, mt))

        maxc = 0
        opt = None
        for jname, card in jobs.items():
            if card > maxc:
                maxc = card
                opt = jname
        # todo: maxc might not be the desired opt meta
        return opt, maxc

    def plan(self):
        for x in self.prereqs:
            x.plan()
        metas = {}
        for x in self.prereqs:
            if x.planned_job is None:
                # reach the end of lineage tree, the end must be either a database table or a local table
                all_ds, card = self._traverse2end(x.df)
                ds_names = []
                for ds in all_ds:
                    if ds.job_name in metas:
                        metas[ds.job_name].append(card)
                    else:
                        metas[ds.job_name] = [card]
                        ds_names.append(ds.job_name)
                x.estimation = card
                x.planned_job = ds_names
            else:
                if x.planned_job in metas:
                    metas[x.planned_job].append(x.estimation)
                metas[x.planned_job] = [x.estimation]

        opt_job, estimated_card = self._max_card(metas)

        # if the dataframe already in the planned data source, we directly link to the previous execution
        # by doing nothing, the prev node will generate sql upto a materialized node
        for x in self.prereqs:
            if isinstance(x.planned_job, list) and opt_job in x.planned_job:
                x.planned_job = opt_job
            elif isinstance(x.planned_job, str) and x.planned_job == opt_job:
                pass
            else:
                # else we do add a data transfer executable first
                # and everything need to be materialized before we do the data transfer
                te = TransferExecutable(x.df, dest=opt_job)
                te.add_prereq(x)
                self.prev.add_prereq(te)
        # assign the estimated job and cardinality to the joint of the branches
        self.prev.estimation = estimated_card
        self.prev.planned_job = opt_job
        if self.prev.df.source is None:
            self.prev.df.set_ds(manager.get_data_source(opt_job))

    def estimate_transfer_amt(self, tbl1: frame.DataFrame, tbl2: frame.DataFrame, count_result: bool = False):
        """
        estimate the amount of tuples need to be transferred back and forth
        @param tbl1: join table1
        @param tbl2: join table2
        @param count_result: whether the result set will be transfered
        @return:
        """
        meta1 = get_meta(tbl1)
        meta2 = get_meta(tbl2)
        if count_result:
            rs_card = estimate_join_card(meta1.nrows, meta2.nrows, )

    def process(self):
        return

    def clear_lineage(self):
        for x in self.prereqs:
            x.clear_lineage()



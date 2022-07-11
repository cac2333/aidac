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


def get_hist(df: frame.DataFrame, col: str):
    if df._data_ is not None:
        total = len(df.data)
        n_null = df.data[col].isnull().sum()
        n_distinct = len(df.data[col].unique())
        hist = Histgram(df.table_name, n_null/total, n_distinct)
    else:
        table_name, null_frac, n_distinct, mcv = df.source.get_hist(df.table_name, col)
        hist = Histgram(table_name, null_frac, n_distinct, mcv)
    return hist


class Executable:
    def __init__(self, df):
        self.df = df
        self.prereqs = []
        self.planned_job = None
        self.estimation = None
        self.rs_required = False

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


class MasterExecutable(Executable):
    def __init__(self):
        self.prereqs = []

    def _get_lowest_cost_path(self, paths):
        lowest, opt_path = paths[0]
        for cost, path in paths:
            if lowest < cost:
                lowest = cost
                opt_path =path
        return opt_path

    def _insert_transfer_block(self, path):
        if path:
            cur = self.prereqs[0]
            while not isinstance(cur, ScheduleExecutable):
                # branches only occurs at the schedule executable block
                assert len(cur.prereqs) <= 1, "None schedule executable block should not have more than 1 prereq"
                cur = cur.prereqs[0]
            # insert transfer block at the schedule block
            dest = path.pop()
            for x in cur.prereqs:
                te = TransferExecutable(x.df, dest=dest)
                te.add_prereq(x)
                cur.prev.add_prereq(te)
                # recursively add transfer blocks down the branch
                self._insert_transfer_block(path)

    def plan(self):
        all_path = super.plan()
        # only need to insert transfer block when other schedule executables are involved
        if all_path:
            path = self._get_lowest_cost_path(all_path)
            self._insert_transfer_block(path)


class ScheduleExecutable(Executable):
    """
        se
        |
    -----------q
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
        if df.source.job_name == LOCAL_DS or df.table_name in df.source.ls_tables():
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
        self.rs_required = self.prev.rs_required

        for x in self.prereqs:
            x.plan()
        metas = {}
        for x in self.prereqs:
            if x.planned_job is None:
                # reach the end of lineage tree, the end must be either a database table or a local table
                all_costs = self.find_all_transfer_n_cost()
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

    def estimate_transfer_costs(self, count_result: bool = False):
        """

        @param count_result:
        @return:
        """
        pass

    def find_local_transfer_costs(self, prev_dest=[]):
        """compute the transfer cost for all possible data transferring at the local level
        all_dest format:
        [cost, all target job destination along the path(e.g. ['job1', 'job2'])]
        """
        # get the merged point transform
        trans = self.prev.df.transform
        all_dest = []
        from aidac.dataframe.transforms import SQLJoinTransform
        # calculate all cost for all possible data transfer
        if isinstance(trans, SQLJoinTransform):
            # we can assume the result set location will be the transfer destination
            # we calculate the cost for all possible path
            # (for every previous path we compute the transfer cost between it and every possible new destination)
            for dest in trans.sources():
                job_name = dest.job_name
                for cost, path in prev_dest:
                    # todo: remove redundant expressions
                    new_path = path + [job_name]
                    new_cost = cost + self.prior_join_cost(trans.sources(), dest)
                    all_dest.append((new_cost, new_path))
        else:
            all_dest = []
        return all_dest

    def prior_join_cost(self, transfer_tables, dest):
        """

        @param transfer_tables:
        @param dest: location where the join take place
        @return: cost the transfer data to perform the join and the result size if transferring back is required
        """
        # collect metas for all tables needs to be transferred and calculate the cost to transfer them
        metas = [get_meta(x) for x in transfer_tables if x.ds.job_name!=dest]
        cost_before = 0
        for meta in metas:
            cost_before = meta.nrows*meta.ncols

        joined_card = 0
        if self.rs_required and dest!=LOCAL_DS:
            joined_card = estimate_join_card(transfer_tables[0], transfer_tables[1])
        return cost_before + joined_card

    def estimate_join_card(self, tbl1, tbl2):
        """
        estimate the amount of tuples need to be transferred back and forth
        @param count_result: whether the result set will be transfered
        @return:
        """
        meta1 = get_meta(tbl1)
        meta2 = get_meta(tbl2)

        trans = self.prev.df.transform

        from aidac.dataframe.transforms import SQLJoinTransform

        assert isinstance(trans, SQLJoinTransform)
        join1_cols, join2_cols = trans.join_cols
        hist1 = get_hist(tbl1, join1_cols)
        hist2 = get_hist(tbl2, join2_cols)

        rs_card = estimate_join_card(meta1.nrows, meta2.nrows, hist1.null_frac, hist2.null_frac, hist1.n_distinct, hist2.n_distinct)
        return rs_card

    def process(self):
        return

    def clear_lineage(self):
        for x in self.prereqs:
            x.clear_lineage()


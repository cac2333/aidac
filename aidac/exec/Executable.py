from __future__ import annotations

import pandas as pd
import numpy as np

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


def get_meta(df: frame.DataFrame):
    if df._data_ is not None:
        meta = MetaInfo(df.columns, len(df.columns), len(df._data_))
        return meta
    else:
        while df.transform is not None:
            df = df.transform.sources()
        nr = df.data_source.row_count(df.table_name)
        meta = MetaInfo(df.columns, len(df.columns), nr)
        return meta


def get_hist(df: frame.DataFrame, col: str):
    if df._data_ is not None:
        total = len(df.data)
        n_null = df.data[col].isnull().sum()
        n_distinct = len(np.unique(df.data[col].to_numpy()))
        hist = Histgram(df.table_name, n_null/total, n_distinct)
    else:
        while df.transform is not None:
            df = df.transform.sources()
        table_name, null_frac, n_distinct, mcv = df.data_source.get_hist(df.table_name, col)
        hist = Histgram(table_name, null_frac, n_distinct, mcv)
    return hist


class Executable:
    def __init__(self, df):
        self.df = df
        self.prereqs = []
        self.planned_job = None
        self.estimation = None
        # result set sent back required
        self.rs_required = False

    def to_be_executed_locally(self, df):
        """
        check a dataframe and its source to see if it can be executed locally
        @return:
        """
        if df.data is not None:
            return True
        if df.transform is not None:
            # as each executable only get to join operations
            if isinstance(df.transform.sources(), tuple):
                for src in df.transform.sources():
                    if not self.to_be_executed_locally(src):
                        return False
            else:
                return self.to_be_executed_locally(df.transform.sources())
        return True
    
    def perform_local_operation(self, df):
        if df.data is not None:
            return df.data

        sources = df.transform.sources()

        # todo: do we also want to save the intermediate results?
        if isinstance(sources, tuple):
            data1 = self.perform_local_operation(sources[0])
            data2 = self.perform_local_operation(sources[1])
            func = getattr(pd.DataFrame, df._saved_func_name_)
            data = func(data1, data2, **df._saved_args_)
        else:
            if sources.data is None:
                data = self.perform_local_operation(sources)
            else:
                data = sources.data
            func = getattr(pd.DataFrame, df._saved_func_name_)
            print(df._saved_args_)
            print(data.columns)
            data = func(data, **df._saved_args_)
        return data

    def process(self):
        """
        Need to process all prerequisites and update the lineage
        @return:
        """
        if self.df.data is not None:
            return self.df.data

        for x in self.prereqs:
            x.process()

        import time
        print('process, planned job={}'.format(self.planned_job))
        start = time.time()
        if self.planned_job == LOCAL_DS:
            assert self.to_be_executed_locally(self.df)
            data = self.perform_local_operation(self.df)
        else:
            sql = self.df.genSQL
            print('sql generated: \n{}'.format(sql))
            print(time.time() - start)
            ds = manager.get_data_source(self.planned_job)
            rs = ds._execute(sql)
            print('returned')
            print(time.time() - start)
            data = rs.get_result_table()
            print(time.time()-start)
            data = pd.DataFrame(data)
        self.clear_lineage()
        self.df._data_ = data
        return data

    def plan(self):
        all_paths = []
        if self.prereqs:
            for x in self.prereqs:
                all_paths.extend(x.plan())
                x.rs_required = self.rs_required
                return all_paths
        else:
            return [(0, Node(self.df.data_source.job_name, None))]

    def add_prereq(self, *other: list[Executable]):
        self.prereqs.extend(other)

    def clear_lineage(self):
        self.df.clear_lineage()

    def pre_process(self):
        for x in self.prereqs:
            x.pre_process()


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
        if src.data_source.job_name == dest:
            return

        scols = src.columns
        # todo: check for duplicate names
        if dest != LOCAL_DS:
            dest_ds = manager.get_data_source(dest)
            dest_ds.create_table(src.table_name, scols)
            dest_ds.import_table(src.table_name, scols, generator(src.data))
        # todo: decide if a local stub should be created

    def removable(self):
        if self.prereqs[0].planned_job == self.dest:
            return self.prereqs[0]

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


class RootExecutable(Executable):
    def __init__(self):
        self.prereqs = []
        self.rs_required = True

    def _get_lowest_cost_path(self, paths):
        lowest, opt_path = paths[0]
        for cost, path in paths:
            if lowest < cost:
                lowest = cost
                opt_path = path
        return opt_path

    def _insert_transfer_block(self, cur: Executable, path: Node):
        if path:
            while not isinstance(cur, ScheduleExecutable):
                # branches only occurs at the schedule executable block
                cur.planned_job = path.val
                if cur.prereqs:
                    cur = cur.prereqs[0]
                else:
                    # reach the end
                    return

                # insert transfer block at the schedule block
            transfer_blocks = []
            for x, p in zip(cur.prereqs, path.children):
                te = TransferExecutable(x.df, prereqs=[x], dest=path.val)
                transfer_blocks.append(te)
                if cur.prev.data_source is None:
                    cur.prev.set_ds(manager.get_data_source(path.val))
                # recursively add transfer blocks down the branch
                self._insert_transfer_block(x, p)
            cur.prereqs.clear()
            cur.prereqs.extend(transfer_blocks)

    def plan(self):
        self.prereqs[0].rs_required = True
        all_path = super().plan()
        # only need to insert transfer block when other schedule executables are involved
        if all_path:
            path = self._get_lowest_cost_path(all_path)
            self._insert_transfer_block(self, path)
            self.pre_process()

    def process(self):
        """
        Need to process all prerequisites and update the lineage
        @return:
        """
        for x in self.prereqs:
            x.process()


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
        self.rs_required = False

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
        if df.data_source.job_name == LOCAL_DS or df.table_name in df.data_source.ls_tables():
            all_source.add(df.data_source)
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
        all_path = []
        for x in self.prereqs:
            path = x.plan()
            all_path.append(path)
        return self.find_local_transfer_costs(all_path)


    def find_local_transfer_costs(self, prev_dest=[]):
        """compute the transfer cost for all possible data transferring at the local level
        all_dest format:
        [cost, all target job destination along the path(e.g. ['job1', 'job2'])]
        """
        # get the merged point transform
        trans = self.prev.transform
        all_dest = []
        from aidac.dataframe.transforms import SQLJoinTransform
        # calculate all cost for all possible data transfer
        if isinstance(trans, SQLJoinTransform):
            # we can assume the result set location will be the transfer destination
            # we calculate the cost for all possible path
            # (for every previous path we compute the transfer cost between it and every possible new destination)
            assert len(self.prereqs) == 2
            plan1 = self.prereqs[0].plan()
            plan2 = self.prereqs[1].plan()

            estimate_card = self.estimate_join_card(self.prereqs[0].df, self.prereqs[1].df)
            all_plans = []
            for cost1, path1 in plan1:
                for cost2, path2 in plan2:
                    # todo: remove redundant expressions
                    if path1.val == path2.val:
                        job_name = path1.val
                        new_path = Node(job_name, [path1, path2])
                        new_cost = cost1 + cost2
                        all_plans.append((new_cost, new_path))
                    else:
                        # path separate as there are two possible destinations
                        new_path = Node(path1.val, [path1, path2])
                        new_cost = cost1 + cost2 + self.prior_join_cost(trans.sources(), (path1.val, path2.val),
                                                                        path1.val, estimate_card)
                        all_plans.append((new_cost, new_path))

                        new_path = Node(path2.val, [path1, path2])
                        new_cost = cost1 + cost2 + self.prior_join_cost(trans.sources(), (path1.val, path2.val),
                                                                        path2.val, estimate_card)
                        all_plans.append((new_cost, new_path))
        else:
            all_plans = [[0, None]]
        return all_plans

    def prior_join_cost(self, transfer_tables, cur_jobs, dest, joined_card):
        """

        @param transfer_tables:
        @param dest: location where the join take place
        @return: cost the transfer data to perform the join and the result size if transferring back is required
        """
        # collect metas for all tables needs to be transferred and calculate the cost to transfer them
        metas = []
        for table, job in zip(transfer_tables, cur_jobs):
            if job != dest:
                metas.append(get_meta(table))

        cost_before = 0
        for meta in metas:
            cost_before = meta.nrows*meta.ncols

        joined_card = joined_card if self.rs_required and dest != LOCAL_DS else 0

        return cost_before + joined_card

    def estimate_join_card(self, tbl1, tbl2):
        """
        estimate the amount of tuples need to be transferred back and forth
        @param count_result: whether the result set will be transfered
        @return:
        """
        meta1 = get_meta(tbl1)
        meta2 = get_meta(tbl2)

        trans = self.prev.transform

        from aidac.dataframe.transforms import SQLJoinTransform

        assert isinstance(trans, SQLJoinTransform)
        join1_cols, join2_cols = trans.join_cols
        hist1 = get_hist(tbl1, join1_cols)
        hist2 = get_hist(tbl2, join2_cols)

        rs_card = estimate_join_card(meta1.nrows, meta2.nrows, hist1.null_frac, hist2.null_frac, hist1.n_distinct, hist2.n_distinct)
        return rs_card

    def pre_process(self):
        new_prereqs = []
        for x in self.prereqs:
            child = x.removable()
            if child:
                self.prereqs.remove(x)
                new_prereqs.append(child)
        self.prereqs.extend(new_prereqs)

    def process(self):
        for x in self.prereqs:
            x.process()

    def clear_lineage(self):
        for x in self.prereqs:
            x.clear_lineage()



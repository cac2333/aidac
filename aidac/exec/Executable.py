from __future__ import annotations

import sys

import pandas as pd
import numpy as np

import time
import random

from typing import Dict
from aidac.common.DataIterator import generator
from aidac.common.column import Column
from aidac.common.meta import MetaInfo, Histgram
from aidac.dataframe import frame
from aidac.dataframe.transforms import SQLProjectionTransform
from aidac.exec.utils import *

from aidac.data_source.DataSourceManager import manager, LOCAL_DS

BOUND1 = 200

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
        # todo: handle empty set, same as the groupby part
        n_null = df.data[col].isnull().sum()
        n_distinct = len(np.unique(df.data[col].to_numpy()))
        hist = Histgram(df.table_name, n_null/total, n_distinct)
    else:
        while df.transform is not None:
            df = df.transform.sources()
        table_name, null_frac, n_distinct, mcv = df.data_source.get_hist(df.table_name, col)
        hist = Histgram(table_name, null_frac, n_distinct, mcv, col)
    return hist


def _random_sampling_string_len(col: pd.Series):
    """
    @param bound1: above which the random sampling should be used
    @param bound2: above which a fixed number of sample points should be used
    @param p: percentage to select
    @return:
    """
    col = col.to_numpy(copy=False)
    total = 0
    if len(col) <= BOUND1:
        for e in col:
            total += sys.getsizeof(e)
        total = total/len(col)
    else:
        idxs = random.sample(range(len(col)), BOUND1)
        for id in idxs:
            total += sys.getsizeof(col[id])
        total = total/BOUND1
    return total


class Executable:
    def __init__(self, df):
        self.df = df
        self.prereqs = []
        self.planned_job = None
        self.estimated_meta = None
        # result set sent back required
        self.rs_required = False
        self.plans = None

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
            if len(data1) == 0 or len(data2)==0:
                print(f"perform locally on empty dataset: {df}")
            func = getattr(pd.DataFrame, df._saved_func_name_)
            data = func(data1, data2, **df._saved_kwargs_)
        else:
            if sources.data is None:
                data = self.perform_local_operation(sources)
            else:
                data = sources.data
            if len(data) == 0:
                print(f"perform locally on empty dataset: {df}")
            func = getattr(data, df._saved_func_name_)
            # print(df._saved_args_)
            # print(data.columns)
            # todo: projection list saved args
            if isinstance(df.transform, SQLProjectionTransform):
                data = func(df._saved_args_, **df._saved_kwargs_)
            else:
                data = func(*df._saved_args_, **df._saved_kwargs_)
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

        print('process, planned job={}'.format(self.planned_job))
        start = time.time()
        if self.planned_job == LOCAL_DS:
            # local pandas operation
            assert self.to_be_executed_locally(self.df)
            data = self.perform_local_operation(self.df)
            print(f'pandas takes time: {time.time()-start}')
        else:
            # materialize remote table

            sql = self.df.genSQL
            # print('sql generated: \n{}'.format(sql))
            ds = manager.get_data_source(self.planned_job)
            print('***************\n'+sql+'\n++**********')
            expl = 'explain analyze ('+sql+')'
            rs = ds._execute(expl)
            print('***************explain****************')
            print(rs.data)

            rs = ds._execute(sql)

            returned = time.time()
            data = rs.to_tb(self.df.columns)
            # get result table and convert to dataframe
            print('sql time = {}, conversion time = {}, total={}'.format(returned-start, time.time()-returned, time.time()-start))
            data = pd.DataFrame(data)
        self.clear_lineage()
        self.df._data_ = data
        from aidac.data_source.DataSource import local_ds
        self.df.set_ds(local_ds)
        return data

    def plan(self, jn_cols=[]):
        if self.plans:
            return self.plans

        all_paths = []
        if self.prereqs:
            for x in self.prereqs:
                all_paths.extend(x.plan(jn_cols))
                x.rs_required = self.rs_required
                self.plans = all_paths
        else:
            # as we have no prereqs, all data has to be in the same database. Thus we can directly use genSQL
            start = time.time()
            est_row, est_width = 0, 0
            if self.df.data is None:
                print(self.df)
                est_row, est_width = \
                    self.df.data_source.get_estimation(self.df.genSQL)
                # print(f'remote estimation takes {time.time()-start}')
            # if the data is local, then we have the actual data
            else:
                est_row = len(self.df.data)
                if est_row > 0:
                    for c in self.df.columns:
                        col = self.df.columns[c]
                        if isinstance(col.dtype, object):
                            est_width += _random_sampling_string_len(self.df.data[c])
                        else:
                            est_width += col.get_size()
                # print(f'local estimation takes {time.time()-start}')

            meta = MetaInfo(self.df.columns, est_width, est_row)
            # estimate histgram of each column
            # todo: this only estimate on the original table
            for c in self.df.columns:
                if c in jn_cols:
                    meta.cmetas[c] = get_hist(self.df, c)
            self.estimated_meta = meta
            # total cost = 0, path = job_name
            # todo: explore all possible data sources
            # print(f'base node plan for {self.df} takes {time.time()-start}')
            self.plans = [(0, Node(self.df.data_source.job_name, None, str(self.df)))]
        return self.plans

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
            start = time.time()
            dest_ds = manager.get_data_source(dest)
            dest_ds.create_table(src.table_name, scols)
            dest_ds.import_table(src.table_name, scols, src.data)
            print('transfer takes time {}'.format(start-time.time()))
        # todo: decide if a local stub should be created

    def removable(self):
        # todo: recursively check the tree
        for x in self.prereqs:
            x.pre_process()
        if self.prereqs[0].planned_job == self.dest:
            return True, self.prereqs[0].prereqs
        else:
            return False, []

    def process(self):
        """
        Need to process all prerequisites and update the lineage
        @return:
        """
        for x in self.prereqs:
            x.process()

        if self.dest is not None:
            self.transfer(self.df, self.dest)

    def plan(self, jn_cols=[]):
        return

    def add_prereq(self, other: Executable):
        self.prereqs.append(other)

    def clear_lineage(self):
        return


class RootExecutable(Executable):
    def __init__(self):
        self.prereqs = []
        self.rs_required = True
        self.plans = None

    def _get_lowest_cost_path(self, paths):
        lowest, opt_path = paths[0]
        for cost, path in paths:
            if cost < lowest:
                lowest = cost
                opt_path = path
        return opt_path, lowest

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
                if cur.prev_ex.planned_job is None:
                    cur.prev_ex.planned_job = path.val
                # recursively add transfer blocks down the branch
                self._insert_transfer_block(x, p)
            cur.prereqs.clear()
            cur.prereqs.extend(transfer_blocks)

    def plan(self, jn_cols=[]):
        self.prereqs[0].rs_required = True
        all_path = super().plan()
        # only need to insert transfer block when other schedule executables are involved
        if all_path:
            path, lowest = self._get_lowest_cost_path(all_path)
            self._insert_transfer_block(self, path)
            self.pre_process()
        print(f'estimated cost: {lowest}')
        return path

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

    def __init__(self, prev, prev_ex):
        self.prev = prev
        self.prev_ex = prev_ex
        self.prereqs = []
        self.rs_required = False
        self.plans = None

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

    def plan(self, jn_cols=[]):
        return self.find_local_transfer_costs(jn_cols)

    def _convert_to_width(self, cols: Dict[str, Column]):
        for cname, col in cols:
            kclass = globals()[str(col.dtype)]()
            sys.getsizeof(kclass)

    def find_local_transfer_costs(self, jn_cols=[]):
        """compute the transfer cost for all possible data transferring at the local level
        all_dest format:
        [cost, all target job destination along the path(e.g. ['job1', 'job2'])]
        """
        # get the merged point transform
        trans = self.prev.transform
        all_dest = []
        from aidac.dataframe.transforms import SQLJoinTransform, SQLProjectionTransform
        # calculate all cost for all possible data transfer
        if isinstance(trans, SQLJoinTransform) or isinstance(trans, SQLProjectionTransform):
            jn_flag = isinstance(trans, SQLJoinTransform)
            # we can assume the result set location will be the transfer destination
            # we calculate the cost for all possible path
            # (for every previous path we compute the transfer cost between it and every possible new destination)
            assert len(self.prereqs) == 2

            plan1 = self.prereqs[0].plan([x[0] for x in trans.join_cols] if jn_flag else [] + jn_cols)
            plan2 = self.prereqs[1].plan([x[1] for x in trans.join_cols] if jn_flag else [] + jn_cols)

            start = time.time()
            # todo: should the prev_ex be assigned this value?
            self.prev_ex.estimated_meta = self.estimate_meta(self.prereqs[0], self.prereqs[1])
            # print(f'estimate for {self.prev} takes {time.time()-start}')

            all_plans = []
            # todo: handle projection where the filter comes from a differebt source
            for cost1, path1 in plan1:
                for cost2, path2 in plan2:
                    # todo: remove redundant expressions
                    if path1.val == path2.val:
                        # the two branches are from the same ds, thus no extra transfer is required
                        job_name = path1.val
                        new_path = Node(job_name, [path1, path2], str(self.prev))
                        new_cost = cost1 + cost2
                        all_plans.append((new_cost, new_path))
                        new_path.jcost = new_cost
                    else:
                        # currently does not support remote filter as the db does not have an order
                        if not jn_flag:
                            raise ValueError('Remote filtering is not supported')
                        # path separate as there are two possible destinations
                        # use path1 as destination
                        new_path = Node(path1.val, [path1, path2], str(self.prev))
                        # todo: write it in a better way
                        new_cost = cost1 + cost2 + self.prior_join_cost(self.prereqs[1], path1.val, path2.val)
                        all_plans.append((new_cost, new_path))
                        new_path.jcost = new_cost

                        new_path = Node(path2.val, [path1, path2], str(self.prev))
                        new_cost = cost1 + cost2 + self.prior_join_cost(self.prereqs[0], path2.val, path1.val)
                        all_plans.append((new_cost, new_path))
                        new_path.jcost = new_cost
        return all_plans

    def prior_join_cost(self, other_table, my_dest, other_dest):
        """
        compute the transferring cost if the other table does not have the same data source destination
        @param other_table: the other table in the join
        @param my_dest: current planned destination
        @param other_dest: the other table's planned destination
        @param joined_card: joined cardinality
        @return:
        """
        if my_dest != other_dest:
            meta = MetaInfo(other_table.df.columns, other_table.estimated_meta.cwidth, other_table.estimated_meta.nrows)
            cost_before = meta.nrows*meta.cwidth
        else:
            cost_before = 0

        est_meta = self.prev_ex.estimated_meta
        joined_card = est_meta.nrows*est_meta.cwidth if my_dest != LOCAL_DS else 0
            # if self.rs_required and my_dest != LOCAL_DS else 0

        return cost_before + joined_card

    def estimate_filter_card(self, trans):
        # todo: validate if the tables exist in that data source
        for s in manager.all_data_sources():
            if s != LOCAL_DS:
                nr, nw = manager.get_data_source(s).get_estimation(trans.genSQL)
        return nr*nw

    def estimate_meta(self, tbl1, tbl2):
        from aidac.dataframe.transforms import SQLJoinTransform
        if isinstance(self.prev.transform, SQLJoinTransform):
            est_row = self.estimate_join_card(tbl1, tbl2)
        else:
            est_row = self.estimate_filter_card(self.prev.transform)
        est_width = 0
        for c in self.prev.columns:
            col = self.prev.columns[c]
            est_width += col.get_size()
        return MetaInfo(self.prev.columns, est_width, est_row)

    def estimate_join_card(self, tbl1, tbl2):
        """
        estimate the amount of tuples need to be transferred back and forth
        @param count_result: whether the result set will be transfered
        @return:
        """
        meta1 = tbl1.estimated_meta
        meta2 = tbl2.estimated_meta

        trans = self.prev.transform

        # choose the column with the largest distinct value to work with
        distinct1, distinct2 = 0, 0

        for jc1, jc2 in trans.join_cols:
            # mismatch distinct values do not matter for now as we only use the biggest value
            if jc1 in meta1.cmetas and jc2 in meta2.cmetas:
                d1 = meta1.cmetas[jc1].n_distinct
                distinct1 = d1 if d1 > distinct1 else distinct1
                d2 = meta2.cmetas[jc2].n_distinct
                distinct2 = d2 if d2 > distinct2 else distinct2

        if distinct1 == 0:
            distinct1 = meta1.nrows

        if distinct2 == 0:
            distinct2 = meta2.nrows
        # todo: use new estimation
        rs_card = estimate_join_card(meta1.nrows, meta2.nrows, 0, 0, distinct1, distinct2)
        return rs_card

    # remove redundant transfer blocks if the dest and the origin are the same
    def pre_process(self):
        new_prereqs = []
        should_remove = []
        for x in self.prereqs:
            can_remove, child = x.removable()
            if can_remove:
                should_remove.append(x)
                new_prereqs.extend(child)
        self.prereqs = [x for x in self.prereqs if x not in should_remove]
        self.prereqs.extend(new_prereqs)

    def process(self):
        for x in self.prereqs:
            x.process()

    def clear_lineage(self):
        for x in self.prereqs:
            x.clear_lineage()



from __future__ import annotations

import sys

import pandas as pd
import numpy as np
import logging

import time
import random

from typing import Dict
from aidac.common.DataIterator import generator
from aidac.common.aidac_types import is_type, ArrayLike
from aidac.common.column import Column
from aidac.common.meta import MetaInfo, Histgram
from aidac.dataframe import frame
from aidac.dataframe.transforms import *
from aidac.exec.utils import *

from aidac.data_source.DataSourceManager import manager, LOCAL_DS

BOUND1 = 1000
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

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


def get_hist(df: frame.DataFrame, col: str):
    if df.data is not None:
        total = len(df.data)
        # todo: handle empty set, same as the groupby part
        if isinstance(df.data, pd.Series):
            n_null = df.data.isnull().sum()
            n_distinct = len(np.unique(df.data.to_numpy()))
        else:
            n_null = df.data[col].isnull().sum()
            n_distinct = len(np.unique(df.data[col].to_numpy()))
        # assume uniform distribution (no histogram bound, mcv, mcf)
        hist = Histgram(df.table_name, col, n_null/total, n_distinct)
    else:
        while df.transform is not None:
            df = df.transform.sources()
            if is_type(df, ArrayLike):
                df = df[0]
        null_frac, n_distinct, mcv, mcf, hist_bounds, avg_width = df.data_source.get_hist(df.table_name, col)
        hist = Histgram(df.table_name, col, null_frac, n_distinct, mcv, mcf, hist_bounds)
    return hist


def _estimate_col_width(df):
    est_width = 0
    for c in df.columns:
        col = df.columns[c]
        logging.debug(f'{c} <{col.dtype}>: {col.get_size()}')
        est_width += col.get_size()
    return est_width


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
        self.plans = None

    def _format_prereqs_str(self):
        children = [str(c) for c in self.prereqs]
        child_lines = [''.join(map(lambda x: '\t' + x, c.splitlines())) for c in children]
        return child_lines

    def __str__(self):
        st = f'Base Block( \n' \
             f'\tdataframe root: {self.df}, \n' \
             f'\tPlanned data source: {self.planned_job}, \n' \
             f'\tEstimated size: ({self.estimated_meta.nrows}, {self.estimated_meta.cwitdh}) \n' \
             f'\tChildren: (\n' \
             f'\t{self._format_prereqs_str()} ' \
             f')'
        return st

    def to_be_executed_locally(self, df):
        """
        check a dataframe and its source to see if it can be executed locally
        @return:
        """
        if df.data is not None:
            return True
        if df.transform is not None:
            # as each executable only get to join operations
            if is_type(df.transform.sources(), ArrayLike):
                for src in df.transform.sources():
                    if not self.to_be_executed_locally(src):
                        return False
            else:
                return self.to_be_executed_locally(df.transform.sources())
        return True

    def _invoke_pd(self, df):
        pass
    
    def perform_local_operation(self, df):
        if df.data is not None:
            return df.data

        sources = df.transform.sources()

        # todo: do we also want to save the intermediate results?
        if is_type(sources, ArrayLike):
            data1 = self.perform_local_operation(sources[0])
            data2 = self.perform_local_operation(sources[1])
            if len(data1) == 0 or len(data2)==0:
                logging.debug(f"perform locally on empty dataset: {df}")
            func = getattr(pd.DataFrame, df._saved_func_name_)
            data = func(data1, data2, **df._saved_kwargs_)
        else:
            if sources.data is None:
                data = self.perform_local_operation(sources)
            else:
                data = sources.data
            if len(data) == 0:
                logging.debug(f"perform locally on empty dataset: {df}")
            func = getattr(data, df._saved_func_name_)
            # logging.debug(df._saved_args_)
            # logging.debug(data.columns)
            # if isinstance(df.transform, SQLProjectionTransform):
            #     data = func(df._saved_args_, **df._saved_kwargs_)
            # else:
            updated_args = list(df._saved_args_)
            for idx, arg in enumerate(df._saved_args_):
                from aidac.dataframe.frame import DataFrame
                if isinstance(arg, DataFrame):
                    # happens only at project. Based on current impl, this has to be local
                    updated_args[idx] = self.perform_local_operation(arg)
            df._saved_args_ = updated_args if updated_args else df._saved_args_

            # in case the function does not return a new dataframe (imply it change the old df in place), we return the old data
            new_data = func(*df._saved_args_, **df._saved_kwargs_)
            data = new_data if new_data is not None else data
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

        # logging.debug('process, planned job={}'.format(self.planned_job))
        start = time.time()
        if self.planned_job == LOCAL_DS:
            # local pandas operation
            assert self.to_be_executed_locally(self.df)
            data = self.perform_local_operation(self.df)
            # logging.debug(f'pandas takes time: {time.time()-start}')
        else:
            # materialize remote table

            sql = self.df.genSQL
            logging.debug('sql generated: \n{}'.format(sql))
            ds = manager.get_data_source(self.planned_job)
            #_logging.debug('***************\n'+sql+'\n++**********')
            # expl = 'explain analyze ('+sql+')'
            # rs = ds._execute(expl)
            # logging.debug('***************\n'+rs+'\n++**********')
            rs = ds._execute(sql)

            returned = time.time()
            data = rs.to_tb(self.df.columns)
            # get result table and convert to dataframe
            logging.debug('sql time = {}, conversion time = {}, total={}'.format(returned-start, time.time()-returned, time.time()-start))
            #todo: q10_v1 local: orders
            # SystemError: <built-in function ensure_datetime64ns> returned a result with an error set
            data = pd.DataFrame(data, columns=self.df.columns.keys())
            # logging.debug(data)
        self.clear_lineage()
        self.df._data_ = data
        from aidac.data_source.DataSource import local_ds
        self.df.set_ds(local_ds)
        return data

    def _get_col_meta(self, collected_cols):
        """
        using get_hist function to get the histogram and mcv .etc for each column
        put in a dictionary with the column name as key and histogram object as the value
        @param collected_cols:
        @return:
        """
        col_meta = {}
        for c in self.df.columns:
            if c in collected_cols:
                col_meta[c] = get_hist(self.df, c)
        return col_meta

    def _local_card_estimate(self, collected_cols):
        """estimate row and column width if the table is materialized"""
        est_row = len(self.df.data)
        est_width = 0
        if est_row > 0:
            for c in self.df.columns:
                col = self.df.columns[c]
                if isinstance(col.dtype, object):
                    # todo: series has shape (n, ), no column names l_extendedprice for binaryoperationTransform
                    if isinstance(self.df.data, pd.Series):
                        est_width += _random_sampling_string_len(self.df.data)
                    else:
                        est_width += _random_sampling_string_len(self.df.data[c])
                else:
                    est_width += col.get_size()
        col_meta = self._get_col_meta(collected_cols)
        meta = MetaInfo(self.df.columns, est_row, est_width, col_meta)
        return meta

    def _collect_fg_cols(self):
        trans = self.df.transform
        cols = []
        while trans is not None and not is_type(trans, SQLJoinTransform):
            if isinstance(trans, SQLFilterTransform):
                cols += list(trans.columns.keys())
            elif isinstance(trans, SQLGroupByTransform):
                for g in trans._groupcols_:
                    cols.append(g)
            if is_type(trans.sources(), ArrayLike):
                trans = trans.sources()[0].transform
            else:
                trans = trans.sources().transform
        return cols

    def _my_estimation(self):
        """
        estimate the row number by inherit prereq's row
        estimate column meta by recompute the column width use stored column information
        # todo: update n_distinct meta and recompute row number as well
        @return:
        """
        pre_meta = self.prereqs[0].estimated_meta
        est_width = _estimate_col_width(self.df)
        est_row = pre_meta.nrows
        cmetas = {}
        for col in self.df.columns:
            if col in pre_meta.cmetas:
                cmetas[col] = pre_meta.cmetas[col]
        return MetaInfo(self.df.columns, est_row, est_width, cmetas)

    def plan(self, jn_cols=[]):
        # avoid repeat plan
        if self.plans:
            return self.plans
        all_paths = []
        # plan for all prereq blocks
        if self.prereqs:
            # collection all filter and groupby columns in this execution block
            collected_cols = self._collect_fg_cols()
            # there should be at most one prereq block, otherwise it wouldn't be a base block
            x = self.prereqs[0]
            all_paths.extend(x.plan(collected_cols + jn_cols))
            meta = self._my_estimation()
            self.plans = all_paths
        else:
            # put the materialization here todo: move this or estimate on local data
            if self.df.data_source is None and self.df.data is None:
                data = self.perform_local_operation(self.df)
                self.clear_lineage()
                self.df._data_ = data
                from aidac.data_source.DataSource import local_ds
                self.df.set_ds(local_ds)

            # local data, directly compute the meta data using pandas
            if self.df.data is not None:
                meta = self._local_card_estimate(jn_cols)
            else:
                # as we have no prereqs, all data has to be in the same database. Thus we can directly use genSQ

                est_row, est_width = \
                    self.df.data_source.get_estimation(self.df.genSQL)
                col_meta = self._get_col_meta(jn_cols)
                meta = MetaInfo(self.df.columns, est_row, est_width, col_meta)
            self.plans = [(0, Node(self.df.data_source.job_name, None, str(self.df)))]
        self.estimated_meta = meta
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

    def __str__(self):
        st = f'Transfer Block( \n' \
             f'\tDataframe root ds: {self.df.data_source.job_name}, \n' \
             f'\tDestination: {self.dest}, \n' \
             f'\tChildren: (\n' \
             f'\t{self._format_prereqs_str()} ' \
             f')'
        return st

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
            if not dest_ds.table_exists(src.table_name):
                dest_ds.create_table(src.table_name, scols)
                dest_ds.import_table(src.table_name, scols, src.data)
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
        self.plans = None
        self.opt_plan = None

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

    def _wrap_return_cost(self, paths):
        """
        add the cost of send data back to the client if the final execution location is not local
        @param paths:
        @return:
        """
        final_meta= self.prereqs[0].estimated_meta
        fcost = final_meta.nrows * final_meta.cwidth
        new_paths = []
        for c, p in paths:
            if p.val == LOCAL_DS:
                new_paths.append((c, p))
            else:
                new_paths.append((c+fcost, p))
        return new_paths

    def _print_all_path(self, paths):
        for c, p in paths:
            logging.debug(f'cost = {c}, path = \n{p}')

    def plan(self, jn_cols=[]):
        all_path = self.prereqs[0].plan()
        # only need to insert transfer block when other schedule executables are involved
        if all_path:
            all_path = self._wrap_return_cost(all_path)
            # self._print_all_path(all_path)
            # logging.debug(f'number of plans: {len(all_path)}')
            path, lowest = self._get_lowest_cost_path(all_path)
            self._insert_transfer_block(self, path)
            self.pre_process()
            self.opt_plan = path
        #_logging.debug(f'estimated cost: {lowest}')
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
        self.plans = None

    def __str__(self):
        st = f'Schedule Block( \n' \
             f'\tDataframe root: {self.prev}, \n' \
             f'\tChildren: (\n' \
             f'\t{self._format_prereqs_str()} ' \
             f')'
        return st

    def plan(self, jn_cols=[]):
        if not self.plans:
            self.plans = self.find_local_transfer_costs(jn_cols)
        return self.plans

    def _convert_to_width(self, cols: Dict[str, Column]):
        for cname, col in cols:
            kclass = globals()[str(col.dtype)]()
            sys.getsizeof(kclass)

    def _concat_path(self, block1, block2, plan1, plan2):
        all_plans = []
        all_ds = manager.all_data_sources()
        for ds in all_ds:
            for c1, p1 in plan1:
                cur_c1 = c1 if ds == p1.val else block1.estimated_meta.to_size + c1
                for c2, p2 in plan2:
                    cur_c2 = c2 if ds == p2.val else block2.estimated_meta.to_size + c2
                    new_path = Node(ds, [p1, p2], str(self.prev))
                    new_cost = cur_c2 + cur_c1
                    all_plans.append((new_cost, new_path))
                    new_path.jcost = new_cost
        return all_plans

    def _extract_jcols(self, idx):
        cols = []
        suffix = self.prev.transform._lsuffix_ if idx == 0 else self.prev.transform._rsuffix_
        for jcol in self.prev.transform.join_cols:
            if jcol[idx] in self.prev.columns or jcol[idx]+suffix in self.prev.columns:
                cols.append(jcol[idx])
        return cols

    def find_local_transfer_costs(self, jn_cols=[]):
        """compute the transfer cost for all possible data transferring at the local level
        all_dest format:
        [cost, all target job destination along the path(e.g. ['job1', 'job2'])]
        """
        # get the merged point transform
        trans = self.prev.transform
        from aidac.dataframe.transforms import SQLJoinTransform, SQLProjectionTransform
        # calculate all cost for all possible data transfer
        if isinstance(trans, SQLJoinTransform) or isinstance(trans, SQLProjectionTransform):
            jn_flag = isinstance(trans, SQLJoinTransform)
            # we can assume the result set location will be the transfer destination
            # we calculate the cost for all possible path
            # (for every previous path we compute the transfer cost between it and every possible new destination)
            assert len(self.prereqs) == 2
            # this will calculate the plan as well as set up the meta information
            plan1 = self.prereqs[0].plan(self._extract_jcols(0) + jn_cols
                                         if jn_flag else jn_cols)
            plan2 = self.prereqs[1].plan(self._extract_jcols(1) + jn_cols
                                         if jn_flag else jn_cols)

            self.estimated_meta = self.estimate_meta(self.prereqs[0], self.prereqs[1])
            # todo: handle projection where the filter comes from a differebt source
            all_plans = self._concat_path(self.prereqs[0], self.prereqs[1], plan1, plan2)
        return all_plans

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

        cmetas = {}
        est_width = _estimate_col_width(self.prev)
        for c in self.prev.columns:
            if c in tbl1.estimated_meta.cmetas:
                cmetas[c] = tbl1.estimated_meta.cmetas[c]
            elif c in tbl2.estimated_meta.cmetas:
                cmetas[c] = tbl2.estimated_meta.cmetas[c]
        return MetaInfo(self.prev.columns, est_row, est_width, cmetas)

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

        # # distinct can be negative ratio or positive number:
        # # todo: move to data source?
        # def convert_distinct(dn, rn):
        #     return -dn * rn if dn < 0 else dn

        for jc1, jc2 in trans.join_cols:
            # mismatch distinct values do not matter for now as we only use the biggest value
            if jc1 in meta1.cmetas and jc2 in meta2.cmetas:
                distinct1 = meta1.cmetas[jc1].n_distinct
                distinct2 = meta2.cmetas[jc2].n_distinct

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



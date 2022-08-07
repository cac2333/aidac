from __future__ import annotations

import datetime
import re
from abc import abstractmethod
import collections

import numpy as np
from typing import Union, List, Dict

from aidac.common.column import Column
from aidac.data_source.DataSource import DataSource, local_ds
from aidac.dataframe.transforms import *
from aidac.exec.Executable import Executable
import pandas as pd
import uuid

import aidac.dataframe.Scheduler as Scheduler

sc = Scheduler.Scheduler()


def create_remote_table(source, table_name):
    return DataFrame(ds=source, table_name=table_name)


def local_frame_wrapper(func):
    def inner(self, *args, **kwargs):
        func_name = func.__name__
        if self.data is not None:
            pd_func = getattr(pd.DataFrame, func_name)
            pdf = pd_func(self.data, *args, **kwargs)
            return DataFrame(pdf, ds=local_ds)
        else:
            df = func(self, *args, **kwargs)
            df._saved_func_name_ = func_name
            df._saved_args_ = args
            df._saved_kwargs_ = kwargs
            return df

    return inner

class DataFrame:
    tid = 1
    def __init__(self, data=None, table_name=None, transform=None, ds=None):
        DataFrame.tid += 1
        tid_str = str(transform) + str(DataFrame.tid)
        self.__tid__ = re.sub(r'\W+', '_', tid_str)
        self.source_table = table_name
        self._transform_ = transform
        self._columns_ = None
        self._ds_ = ds
        self._stubs_ = []
        self._data_ = data
        self._saved_func_name_ = None
        self._saved_args_ = []
        self._saved_kwargs_ = {}

    """
    override so that any unsupported function call directly goes to pandas 
    """

    def __getattr__(self, item):
        def dataframe_wrapper(*args, **kwargs):
            func_name = item
            if self.data is None:
                self.materialize()
            pd_func = getattr(pd.DataFrame, func_name)
            pdf = pd_func(self.data, *args, **kwargs)
            return DataFrame(pdf, ds=local_ds)

        at = dataframe_wrapper
        print('Method/attribute {} is not supported in AIDAC dataframe, '
              'using pandas instead'.format(item))
        return at

    def __str__(self):
        return self.table_name

    @local_frame_wrapper
    def __getitem__(self, key):
        if isinstance(key, DataFrame):
            # todo: selection
            pass
        if isinstance(key, list):
            keys = key
        elif isinstance(key, tuple):
            raise ValueError("Multi-level index is not supported")
        else:
            keys = [key]

        trans = SQLProjectionTransform(self, keys)
        tb = DataFrame(transform=trans, ds=self.data_source)
        return tb

    def materialize(self):
        if self.data is None:
            sc.execute(self)
        return self.data

    @property
    def data(self):
        return self._data_

    def clear_lineage(self):
        del self._transform_
        self._transform_ = None

    def set_ds(self, ds):
        self._ds_ = ds

    @property
    def id(self):
        return self.__tid__

    @property
    def table_name(self):
        return self.source_table if self.source_table else self.__tid__

    @property
    def genSQL(self):
        if self.transform is not None:
            return self.transform.genSQL
        else:
            return 'SELECT * FROM ' + self.table_name

    def add_source(self, ds):
        self._stubs_.append(ds)

    @property
    def shape(self) -> tuple[int, int]:
        pass

    @property
    def transform(self):
        return self._transform_

    @property
    def columns(self):
        if self._columns_ is None:
            if self.data is not None:
                cols = {}
                # create columns using pandas index column name and types
                for cname, ctype in zip(self._data_.dtypes.index, self._data_.dtypes):
                    if isinstance(ctype, np.dtype):
                        ctype = ctype.type
                    cols[cname] = Column(cname, ctype)
                self._columns_ = cols
            else:
                if self.source_table is None:
                    assert self.transform is not None, "A table without a remote database table linked to it must have a transform"
                    # materialize column info
                    self._columns_ = self.transform.columns
                else:
                    cols = self.data_source.table_columns(self.source_table)
                    self._columns_ = collections.OrderedDict()
                    for col in cols:
                        self._columns_[col.name] = col
        return self._columns_

    @property
    def data_source(self):
        if self._ds_:
            return self._ds_
        if self.data is not None:
            self._ds_ = local_ds
        return self._ds_

    def __repr__(self) -> str:
        """
        @return: string representation of current dataframe
        """
        pass

    def merge(self, other, on=None, left_on=None, right_on=None, how='left', suffix=('_x', '_y'), sort=False):
        # local table, eager execution
        if self.data is not None and other.data is not None:
            rs = self.data.merge(other.data, on, how, left_on=left_on, right_on=right_on,
                                 suffix=suffix, sort=sort)
            tb = DataFrame(data=rs, ds=local_ds)
        else:
            func_name = 'merge'
            saved_args = {'on': on, 'how': how, 'left_on': left_on, 'right_on': right_on,
                          'suffixes': suffix, 'sort': sort}
            if left_on and right_on:
                trans = SQLJoinTransform(self, other, left_on, right_on, how, suffix)
            else:
                trans = SQLJoinTransform(self, other, on, on, how, suffix)

            tb = DataFrame(transform=trans)
            tb._saved_func_name_ = func_name
            tb._saved_kwargs_ = saved_args
        return tb

    @local_frame_wrapper
    def fillna(self, col=[], val=0):
        transform = SQLFillNA(self, col, val)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def dropna(self, col=[]):
        transform = SQLDropNA(self, col)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def drop_duplicates(self):
        transform = SQLDropduplicateTransform(self)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def order(self, orderlist: Union[List[str], str]):

        if isinstance(orderlist, str):
            keys = [orderlist]
        else:
            if not orderlist:
                raise ValueError("orderlist cannot be None!")
            keys = orderlist

        transform = SQLOrderTransform(self, keys)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def query(self, expr: str):
        transform = SQLQuery(self, expr)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def apply(self, func, axis=0):
        transform = SQLApply(self, func, axis)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def aggregate(self, projcols, groupcols=None):
        transform = SQLAggregateTransform(self, projcols, groupcols)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def agg(self, func, collist = []):
        trans = SQLAGG_Transform(self, func, collist)
        return DataFrame(self.data_source, transform=trans)

    @local_frame_wrapper
    def head(self, n=5):
        transform = SQLHeadTransform(self, n)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def tail(self, n=5):
        transform = SQLTailTransform(self, n)
        return DataFrame(ds=self.data_source, transform=transform)


    @local_frame_wrapper
    def rename(self, columns: Dict):
        transform = SQLRenameTransform(self, columns)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def groupby(self, by: Union[List[str], str], sort=True, axis = 0):
        if axis == 1:
            raise ValueError("axis = 1 (group by columns) is not currently suppported")
        if isinstance(by, str):
            by_ = [by]
        else:
            if not by:
                raise ValueError("by cannot be empty!")
            by_ = by

        transform = SQLGroupByTransform(self, by_, sort)

        return DataFrame(ds=self.data_source, transform=transform)

    def to_dict(self, orient, into):
        return self._data_.to_dict(orient, into)

    def to_pickle(self, path, compression='infer', protocol=5, storage_options=None):
        return self._data_.to_pickle(path, compression, protocol, storage_options)

    def to_json(self, path_or_buf=None, orient=None, date_format=None, double_precision=10, force_ascii=True,
                date_unit='ms',
                default_handler=None, lines=False, compression="infer", index=True, indent=None, storage_options=None):
        return self._data_.to_json(path_or_buf, orient, date_format, double_precision, force_ascii, date_unit,
                                   default_handler, lines, compression, index, indent, storage_options)

    @local_frame_wrapper
    def __eq__(self, other):
        if isinstance(other, int) or isinstance(other, float) or isinstance(other, str):
            trans = SQLFilterTransform(self, "eq", other)
            return DataFrame(self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __ge__(self, other):
        if isinstance(other, int) or isinstance(other, float) or isinstance(other, str):
            trans = SQLFilterTransform(self, "ge", other)
            return DataFrame(self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __gt__(self, other):
        if isinstance(other, int) or isinstance(other, float) or isinstance(other, str):
            trans = SQLFilterTransform(self, "gt", other)
            return DataFrame(self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __ne__(self, other):
        if isinstance(other, int) or isinstance(other, float) or isinstance(other, str):
            trans = SQLFilterTransform(self, "ne", other)
            return DataFrame(self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __lt__(self, other):
        if isinstance(other, int) or isinstance(other, float) or isinstance(other, str):
            trans = SQLFilterTransform(self, "lt", other)
            return DataFrame(self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __le__(self, other):
        if isinstance(other, int) or isinstance(other, float) or isinstance(other, str):
            trans = SQLFilterTransform(self, "le", other)
            return DataFrame(self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    def to_string(self, buf=None, columns=None, col_space=None, header=True, index=True, na_rep='NaN', formatters=None,
                  float_format=None, sparsify=None, index_names=True, justify=None, max_rows=None, max_cols=None,
                  show_dimensions=False, decimal='.', line_width=None, min_rows=None, max_colwidth=None, encoding=None):
        return self._data_.to_string(buf, columns, col_space, header, index, na_rep, formatters, float_format, sparsify,
                                     index_names, justify, max_rows, max_cols, show_dimensions, decimal, line_width,
                                     min_rows, max_colwidth, encoding)

    def to_csv(self, path_or_buf=None, sep=',', na_rep='', float_format=None, columns=None, header=True, index=True,
               index_label=None, mode='w', encoding=None, compression='infer', quoting=None, quotechar='"',
               line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal='.',
               errors='strict', storage_options=None):
        return self._data_.to_csv(path_or_buf, sep, na_rep, float_format, columns, header, index, index_label, mode,
                                  encoding, compression, quoting, quotechar, line_terminator, chunksize, date_format,
                                  doublequote, escapechar, decimal, errors, storage_options)

    @classmethod
    def read_pickle(cls, filepath_or_buffer, compression, dict__, storage_options):
        return DataFrame(pd.read_pickle(filepath_or_buffer, compression, dict__, storage_options))

    @property
    def table_name(self):
        return self.source_table if self.source_table else self.__tid__

    @classmethod
    def read_orc(cls, path, columns=None, **kwargs):
        return DataFrame(pd.read_orc(columns, **kwargs))


class RemoteTable(DataFrame):
    def __init__(self, source: DataSource = None, transform: Transform = None, table_name: str = None):
        super().__init__(table_name)
        self._ds_ = source
        self.source_table = table_name

        # try retrieve the meta info of the table from data source
        # if table does not exist, an error will occur
        self._link_table_meta()

        self._transform_ = transform
        self._data_ = None
        self.other_ds = []


def read_csv(path, delimiter, header, names) -> DataFrame:
    df = pd.read_csv(path, delimiter=delimiter, header=header, names=names)
    return DataFrame(data=df, ds=local_ds)


def from_dict(data):
    df = pd.DataFrame(data)
    return DataFrame(data=df, ds=local_ds)

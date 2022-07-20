from __future__ import annotations

import logging
from abc import abstractmethod
import collections

import numpy as np
from typing import Union, List

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


class DataFrame:
    def __init__(self, table_name=None, data=None, transform=None, ds=None):
        self.__tid__ = 't_' + uuid.uuid4().hex
        self.source_table = table_name
        self._transform_ = transform
        self._columns_ = None
        self._ds_ = ds
        self._stubs_ = []
        self._data_ = data
        self._saved_func_name_ = None
        self._saved_args_ = {}

    """
    override so that any unsupported function call directly go to pandas 
    """
    def __getattr__(self, item):
        try:
            at = getattr(DataFrame, item)
        except KeyError:
            at = getattr(pd.DataFrame, item)
            logging.info('Method/attribute {} is not supported in AIDAC dataframe, '
                         'using pandas instead'.format(item))
        return at

    def __str__(self):
        return self.table_name

    def __getitem__(self, key):
        func_name = '__getitem__'
        saved_args = {'key': key}

        if self._data_ is not None:
            return DataFrame(data=self._data_[key])

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
        tb._saved_func_name_ = func_name
        tb._saved_args_ = saved_args
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
            tb._saved_args_ = saved_args

        return tb



    """
    All binary algebraic operations may involve data from different data source
    """


    @property
    def shape(self):
        pass

    @abstractmethod
    def vstack(self, othersrclist):
        pass

    @abstractmethod
    def hstack(self, othersrclist, colprefixlist=None):
        pass

    @abstractmethod
    def describe(self):
        pass

    @abstractmethod
    def sum(self, collist=None):
        pass

    @abstractmethod
    def avg(self, collist=None):
        pass

    @abstractmethod
    def count(self, collist=None):
        pass

    @abstractmethod
    def countd(self, collist=None):
        pass

    @abstractmethod
    def countn(self, collist=None):
        pass

    @abstractmethod
    def max(self, collist=None):
        pass

    @abstractmethod
    def min(self, collist=None):
        pass

    @abstractmethod
    def head(self, n=5):
        pass

    @abstractmethod
    def tail(self, n=5):
        pass


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


    def fillna(self, col, val):
        transform = SQLFillNA(self, col, val)
        return RemoteTable(self.data_source, transform)

    def dropna(self, col):
        transform = SQLDropNA(self, col)
        return RemoteTable(self.data_source, transform)

    def drop_duplicates(self):
        transform = SQLDropduplicateTransform(self)
        return RemoteTable(self.data_source, transform)

    def order(self, orderlist: Union[List[str], str]):

        if isinstance(orderlist, str):
            keys = [orderlist]
        else:
            if not orderlist:
                raise ValueError("orderlist cannot be None!")
            keys = orderlist

        transform = SQLOrderTransform(self, keys)
        return RemoteTable(self.data_source, transform)

    def groupby(self, by: Union[List[str], str], groupcols: Union[List[str], str, None]):
        if isinstance(by, str):
            by_ = [by]
        else:
            if not by:
                raise ValueError("by cannot be empty!")
            by_ = by

        if isinstance(groupcols, str):
            groupcols_ = [groupcols]
        else:
            if not groupcols:
                groupcols_ = None
            else:
                groupcols_ = groupcols
        transform = SQLGroupByTransform(self, by_, groupcols_)
        return RemoteTable(self.source, transform)


def read_csv(path, delimiter, header, names) -> DataFrame:
    df = pd.read_csv(path, delimiter=delimiter, header=header, names=names)
    return DataFrame(data=df, ds=local_ds)


def from_dict(data):
    df = pd.DataFrame(data)
    return DataFrame(data=df, ds=local_ds)



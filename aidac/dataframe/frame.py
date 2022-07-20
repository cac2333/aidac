from __future__ import annotations
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
    return RemoteTable(source, table_name=table_name)


class DataFrame:
    def __init__(self, table_name=None):
        self.__tid__ = 't_' + uuid.uuid4().hex
        self.source_table = table_name
        self._transform_ = None
        self._columns_ = None
        self._ds_ = None
        self._stubs_ = []
        self._data_ = None

    def get_data(self):
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
    def shape(self) -> tuple[int, int]:
        pass

    @property
    def transform(self):
        return self._transform_

    @property
    def columns(self):
        if self._columns_ is None:
            cols = {}
            # create columns using pandas index column name and types
            for cname, ctype in zip(self._data_.dtypes.index, self._data_.dtypes):
                if isinstance(ctype, np.dtype):
                    ctype = ctype.type
                cols[cname] = Column(cname, ctype)
            return cols
        return self._columns_

    @property
    def source(self):
        return self._ds_

    def __repr__(self) -> str:
        """
        @return: string representation of current dataframe
        """
        pass

    @abstractmethod
    def filter(self, exp: str):
        pass

    @abstractmethod
    def join(self, other: DataFrame, left_on: list | str, right_on: list | str, join_type: str):
        pass
        """
        May involve mixed data source
        """

    @abstractmethod
    def aggregate(self, projcols, groupcols=None):
        pass

    @abstractmethod
    def project(self, cols: list | str):
        pass

    @abstractmethod
    def order(self, orderlist):
        pass

    @abstractmethod
    def distinct(self):
        pass

    @abstractmethod
    def preview_lineage(self):
        pass

    """
    All binary algebraic operations may involve data from different data source
    """

    @abstractmethod
    def __add__(self, other):
        pass

    @abstractmethod
    def __radd__(self, other):
        pass

    @abstractmethod
    def __mul__(self, other):
        pass

    @abstractmethod
    def __rmul__(self, other):
        pass

    @abstractmethod
    def __sub__(self, other):
        pass

    @abstractmethod
    def __rsub__(self, other):
        pass

    @abstractmethod
    def __truediv__(self, other):
        pass

    @abstractmethod
    def __rtruediv__(self, other):
        pass

    @abstractmethod
    def __pow__(self, power, modulo=None):
        pass

    @abstractmethod
    def __matmul__(self, other):
        pass

    @abstractmethod
    def __rmatmul__(self, other):
        pass

    @property
    @abstractmethod
    def T(self):
        pass

    @abstractmethod
    def __getitem__(self, item):
        pass

    # WARNING !! Permanently disabled  !
    # Weakref proxy invokes this function for some reason, which is forcing the dataframe objects to materialize.
    # @abstractmethod
    # def __len__(self): pass;

    @property
    @abstractmethod
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

    @property
    def data(self):
        return self._data_


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

    @property
    def columns(self):
        if not self._columns_:
            if self.source_table is None:
                assert self.transform is not None, "A table without a remote database table linked to it must have a transform"
                # materialize column info
                self._columns_ = self.transform.columns
            else:
                cols = self.source.table_columns(self.source_table)
                self._columns_ = collections.OrderedDict()
                for col in cols:
                    self._columns_[col.name] = col
        return self._columns_

    def _link_table_meta(self):
        """

        @return:
        """
        pass

    def __str__(self):
        return self.table_name

    def materialize(self):
        self._data_ = sc.execute(self)
        return self._data_

    def fillna(self, col=[], val=0):
        transform = SQLFillNA(self, col, val)
        return RemoteTable(self.source, transform)

    def dropna(self, col=[]):
        transform = SQLDropNA(self, col)
        return RemoteTable(self.source, transform)

    def drop_duplicates(self):
        transform = SQLDropduplicateTransform(self)
        return RemoteTable(self.source, transform)

    def order(self, orderlist: Union[List[str], str]):

        if isinstance(orderlist, str):
            keys = [orderlist]
        else:
            if not orderlist:
                raise ValueError("orderlist cannot be None!")
            keys = orderlist

        transform = SQLOrderTransform(self, keys)
        return RemoteTable(self.source, transform)

    def query(self, expr: str):
        transform = SQLQuery(self, expr)
        return RemoteTable(self.source, transform)

    def apply(self, func, axis=0):
        transform = SQLApply(self, func, axis)
        return RemoteTable(self.source, transform)

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

    def __getitem__(self, key):
        if self._data_ is not None:
            return self._data_[key]

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
        return RemoteTable(self.source, trans)

    def merge(self, other, on=None, left_on=None, right_on=None, how='left', suffix=('_x', '_y'), sort=False):
        if left_on and right_on:
            trans = SQLJoinTransform(self, other, left_on, right_on, how, suffix)
        else:
            trans = SQLJoinTransform(self, other, on, on, how, suffix)
        return RemoteTable(transform=trans)

    def aggregate(self, projcols, groupcols=None):
        transform = SQLAggregateTransform(self, projcols, groupcols)
        return RemoteTable(self.source, transform)

    def head(self, n=5):
        transform = SQLHeadTransform(self, n)
        return RemoteTable(self.source, transform)

    def tail(self, n=5):
        transform = SQLTailTransform(self, n)
        return RemoteTable(self.source, transform)

    def rename(self, columns: Dict):
        transform = SQLRenameTransform(self, columns)
        return RemoteTable(self.source, transform)

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
        self.other_sources.append(ds)


def read_csv(path, delimiter, header, names) -> LocalTable:
    df = pd.read_csv(path, delimiter=delimiter, header=header, names=names)
    return LocalTable(df)


def from_dict(data):
    df = pd.DataFrame(data)
    return LocalTable(df)


class LocalTable(DataFrame):
    def __init__(self, data, table_name=None):
        super().__init__(table_name)
        self._data_ = data
        self._ds_ = local_ds
        self._stubs_ = []

    @property
    def genSQL(self):
        return 'SELECT * FROM ' + self.table_name

    def merge(self, other, on=None, left_on=None, right_on=None, how='left', suffix=('_x', '_y'), sort=False):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.merge(other, on, how, left_on=left_on, right_on=right_on,
                                                suffix=suffix, sort=sort))
        else:
            if left_on and right_on:
                trans = SQLJoinTransform(self, other, left_on, right_on, how, suffix)
            else:
                trans = SQLJoinTransform(self, other, on, on, how, suffix)
            return RemoteTable(transform=trans)

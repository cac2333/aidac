from __future__ import annotations
from typing import *
import pandas as pd
import numpy as np
import re

from aidac.common.column import Column
from aidac.dataframe import DataFrame


class LocalTable(DataFrame):
    def __init__(self, data: pd.DataFrame, table_name=None):
        super().__init__(table_name)
        self._data_ = data
        self._stub_ = None

    @classmethod
    def read_csv(cls, path, delimiter, header) -> LocalTable:
        df = pd.read_csv(path, delimiter=delimiter, header=header)
        return LocalTable(df)

    @classmethod
    def join(self, other, left_on: Union[list, str], right_on: Union[list, str], join_type: str):
        '''

        :param other: saved for later (remote table types)
        :param left_on:
        :param right_on:
        :param join_type:
        :return:
        '''
        pass

    @property
    def columns(self):
        cols = {}
        # create columns using pandas index column name and types
        for cname, ctype in zip(self._data_.dtypes.index, self._data_.dtypes):
            cols[cname] = Column(cname, ctype)
        return cols

    def __repr__(self) -> str:
        """
        @return: string representation of current dataframe
        """
        return self._data_.__repr__()

    def filter(self, exp: Union[str, list], axis=0):
        if isinstance(exp, str):
            try:
                re.compile(exp)
                is_valid = True
            except re.error:
                is_valid = False
            if is_valid:
                return LocalTable(self._data_.filter(regex=exp, axis=axis))
            return LocalTable(self._data_.filter(like=exp, axis=axis))
        return LocalTable(self._data_.filter(items=exp, axis=axis))

    def aggregate(self, projcols, groupcols=None):
        pass

    def project(self, cols: Union[List, str]):
        # if isinstance(cols, list):
        return LocalTable(self._data_[cols])
        # return LocalTable(self._data_)

    def order(self, orderlist):
        return LocalTable(self._data_.sort_values(by=orderlist))


    def distinct(self):
        pass

    def preview_lineage(self):
        pass

    """
    All binary algebraic operations may involve data from different data source
    """

    def __add__(self, other):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__add__(other._data_))

    def __radd__(self, other):
        if isinstance(other, LocalTable):
            return self.__add__(other)

    def __mul__(self, other):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__mul__(other._data_))

    def __rmul__(self, other):
        if isinstance(other, LocalTable):
            return self.__mul__(other)

    def __sub__(self, other):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__sub__(other._data_))

    def __rsub__(self, other):
        if isinstance(other, LocalTable):
            return LocalTable(other._data_.__sub__(self._data_))

    def __truediv__(self, other):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__truediv__(other._data_))

    def __rtruediv__(self, other):
        if isinstance(other, LocalTable):
            return LocalTable(other._data_.__truediv__(self._data_))

    def __pow__(self, power, modulo=None):
        if not modulo:
            return LocalTable(self._data_**power)
        return LocalTable(self._data_**power % modulo)

    def __matmul__(self, other):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__matmul__(other._data_))

    def __rmatmul__(self, other):
        if isinstance(other, LocalTable):
            return LocalTable(other._data_.__matmul__(self._data_))

    @property
    def T(self):
        return LocalTable(self._data_.T)

    def __getitem__(self, item):
        return LocalTable(self._data_.__getitem__(item))
    # def __setitem__(self, key, value):
    # WARNING !! Permanently disabled  !
    # Weakref proxy invokes this function for some reason, which is forcing the dataframe objects to materialize.
    # @abstractmethod
    # def __len__(self): pass;

    @property
    def shape(self) -> Tuple[int, int]:
        return self._data_.shape

    def vstack(self, othersrclist):
        self._data_.concat(othersrclist)

    def hstack(self, othersrclist, colprefixlist=None):
        pass

    def describe(self):
        return self._data_.describe()

    def sum(self, collist=None):
        return self._data_[collist].sum()

    def avg(self, collist=None):
        return self._data_[collist].avg()

    def count(self, collist=None):
        return self._data_[collist].count()


    def countd(self, collist=None):
        pass

    def countn(self, collist=None):
        pass

    def max(self, collist=None):
        return self._data_[collist].max()

    def min(self, collist=None):
        return self._data_[collist].min()

    def head(self, n=5):
        return LocalTable(self._data_.head(n))

    def tail(self, n=5):
        return LocalTable(self._data_.tail(n))
    #
    # def duplicated(self):
    #     pass
    #
    # def sort_values(self):
    #     pass
    #
    # def append(self):
    #     pass
    #
    # def transpose(self):
    #     return LocalTable(self._data_.T)
    #
    # def shape(self):
    #     pass
    #
    # def apply(self):
    #     pass

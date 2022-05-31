from __future__ import annotations

from math import nan
from typing import *
import pandas as pd
import numpy as np
import re

from pandas._libs.lib import NoDefault

from aidac.common.column import Column
from aidac.dataframe import DataFrame


# TODO: add compatible for other DataFrame types

class LocalTable(DataFrame):
    def __init__(self, data: pd.DataFrame, table_name=None):
        super().__init__(table_name)
        self._data_ = data
        self._stub_ = None

    def get_data(self):
        return self._data_.copy(True)
    @classmethod
    def read_csv(cls, path, delimiter, header) -> LocalTable:
        df = pd.read_csv(path, delimiter=delimiter, header=header)
        return LocalTable(df)

    @classmethod
    def read_pickle(cls, filepath_or_buffer, compression, dict__, storage_options):
        return LocalTable(pd.read_pickle(filepath_or_buffer, compression, dict__, storage_options))

    @classmethod
    def read_orc(cls, path, columns=None, **kwargs):
        return LocalTable(pd.read_orc(columns, **kwargs))

    @classmethod
    def read_sql_table(cls, table_name, con, schema = None, index_col = None, coerce_float = True, parse_dates = None, columns = None, chunksize = None):
        return LocalTable(pd.read_sql_table(table_name, con, schema, index_col, coerce_float, parse_dates, columns, chunksize))

    @classmethod
    def read_sql_query(cls, sql, con, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None,
                   dtype=None):
        return LocalTable(pd.read_sql_query(sql, con, index_col, coerce_float, params, parse_dates, chunksize, dtype))

    @classmethod
    def read_sql(cls, sql, con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None):
        return LocalTable(pd.read_sql(sql, con, index_col, coerce_float, params, parse_dates, columns, chunksize))

    def to_sql(self, name, con, schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None,
           method=None):
        return self._data_.to_sql(name, con, schema, if_exists, index, index_label, chunksize, dtype, method)

    def to_dict(self, orient, into):
        return self._data_.to_dict(orient, into)

    def to_pickle(self, path, compression='infer', protocol=5, storage_options=None):
        return self._data_.to_pickle(path, compression, protocol, storage_options)

    def to_json(self, path_or_buf=None, orient=None, date_format=None, double_precision=10, force_ascii=True, date_unit='ms',
            default_handler=None, lines=False, compression = "infer", index=True, indent=None, storage_options=None):
        return self._data_.to_json(path_or_buf, orient, date_format, double_precision, force_ascii, date_unit, default_handler, lines, compression, index, indent, storage_options)

    def to_string(self, buf=None, columns=None, col_space=None, header=True, index=True, na_rep='NaN', formatters=None, float_format=None, sparsify=None, index_names=True, justify=None, max_rows=None, max_cols=None, show_dimensions=False, decimal='.', line_width=None, min_rows=None, max_colwidth=None, encoding=None):
       return self._data_.to_string(buf, columns, col_space, header, index, na_rep, formatters, float_format, sparsify, index_names, justify, max_rows, max_cols, show_dimensions, decimal, line_width, min_rows, max_colwidth, encoding)

    def to_csv(self, path_or_buf=None, sep=',', na_rep='', float_format=None, columns=None, header=True, index=True, index_label=None, mode='w', encoding=None, compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal='.', errors='strict', storage_options=None):
        return self._data_.to_csv(path_or_buf, sep, na_rep, float_format, columns, header, index, index_label, mode, encoding, compression, quoting, quotechar, line_terminator, chunksize, date_format, doublequote, escapechar, decimal, errors, storage_options)



    @property
    def index(self):
        return self._data_.index

    #
    # def columns(self):
    #     return self._data_.columns
    @property
    def dtypes(self):
        return self._data_.dtypes

    @property
    def values(self):
        return self._data_.values

    @property
    def axes(self):
        return self._data_.axes

    @property
    def ndim(self):
        return self._data_.ndim

    @property
    def size(self):
        return self._data_.size

    @property
    def shape(self):
        return self._data_.shape

    def at(self, index, column):
        return self._data_.at[index, column]

    def iat(self, index: int, column: int):
        return self._data_.at[index, column]

    def memory_usage(self, index=True, deep=False):
        return self._data_.memory_usage(index, deep)


    def head(self, n=5):
        return LocalTable(self._data_.head(n))


    def tail(self, n=5):
        return LocalTable(self._data_.tail(n))


    def loc(self, label: Union[int, str, list, Dict[Union[int, str], Union[int, str]], Callable[Any, bool]]):
        return LocalTable(self._data_.loc(label))


    def iloc(self, label: Union[int, Dict[int, int], List[Union[int, bool]], Callable[Any, bool]]):
        return LocalTable(self._data_.iloc(label))


    def insert(self, loc, column, value, allow_duplicates=False):
        self._data_.insert(loc, column, value, allow_duplicates)

    def add_prefix(self, prefix):
        return LocalTable(self._data_.add_prefix(prefix))

    def add_suffix(self, suffix):
        return LocalTable(self._data_.add_suffix(suffix))

    def __iter__(self):
        return self._data_.__iter__()

    def items(self):
        return self._data_.items()

    def iteritems(self):
        return self._data_.iteritems()

    def iterrows(self):
        return self._data_.iterrows()

    def keys(self):
        return self._data_.keys()

    def itertuples(self, index=True, name=None):
        return self._data_.itertuples(index, name)

    def pop(self, label):

        return self._data_.pop(item=label)

    def get(self, key, default=None):
        return LocalTable(self._data_.get(key, default))

    def isin(self, values):
        return LocalTable(self._data_.isin(values))

    def where(self, cond, other:DataFrame, inplace=False, axis=None, level=None, errors='raise',
              try_cast=NoDefault.no_default):
        return self._data_.where(cond, other.get_data(), inplace, axis, level, errors, try_cast)

    def align(self, other:DataFrame, join='outer', axis=None, level=None, copy=True, fill_value=None, method=None, limit=None,
              fill_axis=0,
              broadcast_axis=None):
        return LocalTable(
            self._data_.align(other.get_data(), join, axis, level, copy, fill_value, method, limit, fill_axis, broadcast_axis))

    def at_time(self, time, asof=False, axis=None):
        return LocalTable(self._data_.at_time(time, asof, axis))

    def between_time(self, start_time, end_time, include_start=NoDefault.no_default, include_end=NoDefault.no_default,
                     inclusive=None, axis=None):
        return LocalTable(self._data_.between_time(start_time, end_time, include_start, include_end, inclusive, axis))

    def drop(self, labels=None, axis=0, index=None, columns=None, level=None, inplace=False, errors='raise'):
        return LocalTable(self._data_.drop(labels, axis, index, columns, level, inplace, errors))

    def drop_duplicates(self, subset=None, keep='first', inplace=False, ignore_index=False):
        return LocalTable(self._data_.drop_duplicates(self, keep, inplace, ignore_index))

    def duplicated(self, subset=None, keep='first'):
        return self._data_.duplicated(subset, keep)

    def equals(self, other):
        return self._data_.equals(other.get_data())

    def first(self, offset):
        return LocalTable(self._data_.first(offset))

    def last(self, offset):
        return LocalTable(self._data_.last(offset))

    def query(self, expr, inplace=False, **kwargs):
        return LocalTable(self._data_.query(expr, inplace, **kwargs))

    def apply(self, func, axis=0, raw=False, result_type=None, args=(), **kwargs):
        return LocalTable(self._data_.apply(func, axis, raw, result_type, args, **kwargs))

    def applymap(self, func, na_action=None, **kwargs):
        return LocalTable(self._data_.applymap(func, na_action, **kwargs))

    def pipe(self, func, *args, **kwargs):
        return LocalTable(self._data_.pipe(func, *args, **kwargs))

    def reindex(self, labels=None, index=None, columns=None, axis=None, method=None, copy=True, level=None,
                fill_value=nan, limit=None, tolerance=None):
        return LocalTable(
            self._data_.reindex(labels, index, columns, axis, method, copy, level, fill_value, level, limit, tolerance))

    def rename(self, mapper=None, *, index=None, columns=None, axis=None, copy=True, inplace=False, level=None,
               errors='ignore'):
        return LocalTable(
            self._data_.rename(mapper, index=index, columns=columns, axis=axis, copy=copy, level=level, errors=errors))

    def rename_axis(self, mapper=None, index=None, columns=None, axis=None, copy=True, inplace=False):
        return LocalTable(self._data_.rename_axis(mapper, index, columns, axis, copy, inplace))

    def reset_index(self, level=None, drop=False, inplace=False, col_level=0, col_fill=''):
        return LocalTable(self._data_.reset_index(level, drop, inplace, col_level, col_fill))

    def sample(self, n=None, frac=None, replace=False, weights=None, random_state=None, axis=None, ignore_index=False):
        return LocalTable(self._data_.sample(n, frac, replace, weights.weights, random_state, axis, ignore_index))

    def set_axis(self, labels, axis=0, inplace=False):
        return LocalTable(self._data_.set_axis(labels, axis, inplace))

    def set_index(self, keys, drop=True, append=False, inplace=False, verify_integrity=False):
        return LocalTable(self._data_.set_index(keys, drop, append, inplace, verify_integrity))

    def take(self, indices, axis=0, is_copy=None, **kwargs):
        return LocalTable(self._data_.take(indices, axis, is_copy, **kwargs))

    def truncate(self, before=None, after=None, axis=None, copy=True):
        return LocalTable(self._data_.truncate(before, after, axis, copy))

    def backfill(self, axis=None, inplace=False, limit=None, downcast=None):
        return LocalTable(self._data_.backfill(axis, inplace, limit, downcast))

    def dropna(self, axis=0, how='any', thresh=None, subset=None, inplace=False):
        return LocalTable(self._data_.dropna(axis, how, thresh, subset, inplace))

    def fillna(self, value=None, method=None, axis=None, inplace=False, limit=None, downcast=None):
        return LocalTable(self.fillna(value, method, axis, inplace, limit, downcast))

    def interpolate(self, method='linear', axis=0, limit=None, inplace=False, limit_direction=None, limit_area=None,
                    downcast=None, **kwargs):
        return LocalTable(
            self._data_.interpolate(method, axis, limit, inplace, limit_direction, limit_area, downcast, **kwargs))

    def isna(self):
        return LocalTable(self._data_.isna())

    def isnull(self):
        return LocalTable(self._data_.isnull())

    def notna(self):
        return LocalTable(self._data_.notna())

    def notnull(self):
        return LocalTable(self._data_.notnull())

    def pad(self, axis=None, inplace=False, limit=None, downcast=None):
        return LocalTable(self._data_.pad(axis, inplace, limit, downcast))

    def replace(self, to_replace=None, value=NoDefault.no_default, inplace=False, limit=None, regex=False,
                method=NoDefault.no_default):
        return LocalTable(self._data_.replace(to_replace, value, inplace, limit, regex, method))

    def droplevel(self, level, axis=0):
        return LocalTable(self._data_.droplevel(level, axis))

    def pivot(self, index=None, columns=None, values=None):
        return LocalTable(self._data_.pivot(index, columns, values))

    def pivot_table(self, values=None, index=None, columns=None, aggfunc='mean', fill_value=None, margins=False,
                    dropna=True, margins_name='All', observed=False, sort=True):
        return LocalTable(
            self._data_.pivot_table(values, index, columns, aggfunc, fill_value, margins, dropna, margins_name,
                                    observed, sort))

    def sort_values(self, by, axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last',
                    ignore_index=False,
                    key=None):
        return LocalTable(self._data_.sort_values(by, axis, ascending, inplace, kind, na_position, ignore_index, key))

    def sort_index(self, axis=0, level=None, ascending=True, inplace=False, kind='quicksort', na_position='last',
                   sort_remaining=True, ignore_index=False, key=None):
        return LocalTable(
            self._data_.sort_index(axis, level, ascending, inplace, kind, na_position, sort_remaining, ignore_index,
                                   key))

    def squeeze(self, axis=None):
        return LocalTable(self._data_.squeeze(axis))

    @classmethod
    # def join(self, other, left_on: Union[list, str], right_on: Union[list, str], join_type: str):
        # '''
        #
        # :param other: saved for later (remote table types)
        # :param left_on:
        # :param right_on:
        # :param join_type:
        # :return:
        # '''
        # pass

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

    def aggregate(self, func=None, axis=0, *args, **kwargs):
        return LocalTable(self._data_.aggregate(func, axis, *args, **kwargs))

    def transform(self, func, axis=0, *args, **kwargs):
        return LocalTable(self._data_.transform(func, axis, args, kwargs))

    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True, group_keys=True,
                squeeze=NoDefault.no_default, observed=False, dropna=True):
        return self._data_.groupby(by, axis, level, as_index, sort, group_keys, squeeze, observed, dropna)

    def rolling(self, window, min_periods=None, center=False, win_type=None, on=None, axis=0, closed=None,
                method='single'):
        return self._data_.rolling(window, min_periods, center, win_type, on, axis, closed, method)

    def expanding(self, min_periods=1, center=None, axis=0, method='single'):
        return self._data_.expanding(min_periods, center, axis, method)

    def project(self, cols: Union[List, str]):
        # if isinstance(cols, list):
        return LocalTable(self._data_[cols])
        # return LocalTable(self._data_)

    def order(self, orderlist):
        return LocalTable(self._data_.sort_values(by=orderlist))

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False, sort=False,
          suffixes=('_x', '_y'), copy=True, indicator=False, validate=None):
        return LocalTable(self._data_.merge(right, how, on, left_on, right_on, left_index, right_index, sort, suffixes, copy, indicator, validate))

    def update(self, other, join='left', overwrite=True, filter_func=None, errors='ignore'):
        self._data_.update(other.get_data, join, overwrite, filter_func, errors)

    def attrs(self):
        return self._data_.attrs




    def distinct(self):
        pass

    ##

    def preview_lineage(self):
        pass

    """
    All binary algebraic operations may involve data from different data source
    """
    ############################################################
    '''
    The below methods return Localtable with dataframes of bool
    '''

    def __lt__(self, other:DataFrame):
        return LocalTable(self._data_.__lt__(other.get_data()))

    def __gt__(self, other:DataFrame):
        return LocalTable(self._data_.__gt__(other.get_data()))

    def __le__(self, other:DataFrame):
        return LocalTable(self._data_.__le__(other.get_data()))

    def __ge__(self, other:DataFrame):
        return LocalTable(self._data_.__ge__(other.get_data()))

    def __ne__(self, other:DataFrame):
        return LocalTable(self._data_.__ne__(other.get_data()))

    def __eq__(self, other:DataFrame):
        return LocalTable(self._data_.__eq__(other.get_data()))

    #####################################

    def __add__(self, other:DataFrame):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__add__(other._data_))

    def __radd__(self, other):
        if isinstance(other, LocalTable):
            return self.__add__(other.get_data())

    def __mul__(self, other:DataFrame):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__mul__(other._data_))

    def __rmul__(self, other:DataFrame):
        if isinstance(other, LocalTable):
            return self.__mul__(other._data_)

    def __sub__(self, other:DataFrame):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__sub__(other._data_))

    def __rsub__(self, other:DataFrame):
        if isinstance(other, LocalTable):
            return LocalTable(other._data_.__sub__(self._data_))

    def __truediv__(self, other:DataFrame):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__truediv__(other._data_))

    def __rtruediv__(self, other:DataFrame):
        if isinstance(other, LocalTable):
            return LocalTable(other._data_.__truediv__(self._data_))

    def __pow__(self, power, modulo=None):
        if not modulo:
            return LocalTable(self._data_ ** power)
        return LocalTable(self._data_ ** power % modulo)

    def __matmul__(self, other:DataFrame):
        if isinstance(other, LocalTable):
            return LocalTable(self._data_.__matmul__(other._data_))

    def __rmatmul__(self, other:DataFrame):
        if isinstance(other, LocalTable):
            return LocalTable(other._data_.__matmul__(self._data_))

    @property
    def T(self):
        return LocalTable(self._data_.T)

    def compare(self, other:DataFrame, align_axis=1, keep_shape=False, keep_equal=False):
        return LocalTable(self._data_.compare(other.get_data(), align_axis, keep_shape, keep_equal))

    def join(self, other:DataFrame, on=None, how='left', lsuffix='', rsuffix='', sort=False):
        return LocalTable(self._data_.join(other.get_data(), on, how, lsuffix, rsuffix, sort))


    def combine(self, other:DataFrame, func, fill_value=None, overwrite=True):
        return LocalTable(self._data_.combine(other.get_data(), func, fill_value, overwrite))

    def combine_first(self, other:DataFrame):
        return LocalTable(self._data_.combine_first(other.get_data()))

    def __getitem__(self, item):
        return LocalTable(self._data_.__getitem__(item))

    # def __setitem__(self, key, value):
    # WARNING !! Permanently disabled  !
    # Weakref proxy invokes this function for some reason, which is forcing the dataframe objects to materialize.
    # @abstractmethod
    # def __len__(self): pass;

    def vstack(self, othersrclist):
        self._data_.concat(othersrclist, axis=0)

    def hstack(self, othersrclist, colprefixlist=None):
        self._data_.concat(othersrclist, axis=1)

    def describe(self):
        return self._data_.describe()

    def std(self, axis=None, skipna=True, level=None, ddof=1, numeric_only=None, **kwargs):
        return LocalTable(self._data_.std(axis, skipna, level, ddof, numeric_only, **kwargs))

    def sum(self, axis=None, skipna=True, level=None, numeric_only=None, min_count=0, **kwargs):
        return LocalTable(self._data_.sum(axis, skipna, level, numeric_only, min_count, **kwargs))

    def idxmax(self, axis=0, skipna=True):
        return self._data_.idxmax(axis, skipna)

    def idxmin(self, axis=0, skipna=True):
        return self._data_.idxmin(axis, skipna)

    # def sum(self, collist=None):
    #     return self._data_[collist].sum()

    def avg(self, collist=None):
        return self._data_[collist].avg()

    def count(self, axis=0, level=None, numeric_only=False):
        return LocalTable(self._data_.count(axis, level, numeric_only))

    def mod(self, other:DataFrame, axis='columns', level=None, fill_value=None):
        return LocalTable(self._data_.__mod__(other.get_data(), axis, level, fill_value))

    def dot(self, other):
        return LocalTable(self._data_.dot(other.get_data()))

    def abs(self):
        return LocalTable(self._data_.abs())

    def all(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        return LocalTable(self._data_.all(axis, bool_only, skipna, level, **kwargs))

    def any(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        return LocalTable(self._data_.any(axis, bool_only, skipna, level, **kwargs))

    def clip(self, alower=None, upper=None, axis=None, inplace=False, *args, **kwargs):
        return LocalTable(self._data_.clip(alower, upper, axis, inplace, *args, **kwargs))

    def cummax(self, axis=None, skipna=True, *args, **kwargs):
        return LocalTable(self._data_.cummax(axis, skipna, *args, **kwargs))

    def cummin(self, axis=None, skipna=True, *args, **kwargs):
        return LocalTable(self._data_.cummax(axis, skipna, *args, **kwargs))

    def cumprod(self, axis=None, skipna=True, *args, **kwargs):
        return LocalTable(self._data_.cumprod(axis, skipna, *args, **kwargs))

    def cumsum(self, axis=None, skipna=True, *args, **kwargs):
        return LocalTable(self._data_.cumsum(axis, skipna, *args, **kwargs))

    def diff(self, periods=1, axis=0):
        return LocalTable(self._data_.diff(periods, axis))

    def eval(self, expr, inplace=False, **kwargs):
        return LocalTable(self._data_.eval(expr, inplace, **kwargs))

    def max(self, axis=NoDefault.no_default, skipna=True, level=None, numeric_only=None, **kwargs):
        return LocalTable(self._data_.max(axis, skipna, level, numeric_only, **kwargs))

    def min(self, axis=NoDefault.no_default, skipna=True, level=None, numeric_only=None, **kwargs):
        return LocalTable(self._data_.min(axis, skipna, level, numeric_only, **kwargs))

    def mean(self, axis=NoDefault.no_default, skipna=True, level=None, numeric_only=None, **kwargs):
        return LocalTable(self._data_.mean(axis, skipna, level, numeric_only, **kwargs))

    def median(self, axis=NoDefault.no_default, skipna=True, level=None, numeric_only=None, **kwargs):
        return LocalTable(self._data_.median(axis, skipna, level, numeric_only, **kwargs))

    def mode(self, axis=0, numeric_only=False, dropna=True):
        return LocalTable(self._data_.mode(axis, numeric_only, dropna))

    def product(self, axis=None, skipna=True, level=None, numeric_only=None, min_count=0, **kwargs):
        return LocalTable(self._data_.product(axis, skipna, level, numeric_only, min_count, **kwargs))

    def round(self, decimals=0, *args, **kwargs):
        return LocalTable(self._data_.round(decimals, *args, **kwargs))

    def ewm(self, com=None, span=None, halflife=None, alpha=None, min_periods=0, adjust=True, ignore_na=False, axis=0,
            times=None,
            method='single'):
        return self._data_.ewm(com, span, halflife, alpha, min_periods, adjust, ignore_na, axis, times, method)

    def nlargest(self, n, columns, keep='first'):
        return LocalTable(self._data_.nlargest(n, columns, keep))

    def nsmallest(self, n, columns, keep='first'):
        return LocalTable(self._data_.nsmallest(n, columns, keep))

    def stack(self, level=- 1, dropna=True):
        return LocalTable(self._data_.stack(level, dropna))

    def unstack(self, level=- 1, fill_value=None):
        return LocalTable(self._data_.unstack(level, fill_value))


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

from __future__ import annotations

import datetime
import numbers
import re
import warnings
from abc import abstractmethod
import collections

import numpy as np
from typing import Union, List, Dict

from aidac.common.aidac_types import *
from _distutils_hack import override

from aidac.common.column import Column
from aidac.data_source.DataSource import DataSource, local_ds
from aidac.dataframe.transforms import *
from aidac.exec.Executable import Executable
import pandas as pd
import uuid

import aidac.dataframe.Scheduler as Scheduler

sc = Scheduler.Scheduler()


STR_FUNCS = {'contains'}

def local_frame_wrapper(func):
    def inner(self, *args, **kwargs):
        func_name = func.__name__
        if self.data is not None:
            if isinstance(self.data, pd.Series) and func_name in STR_FUNCS:
                pd_func = getattr(self.data.str, func_name)
            else:
                pd_func = getattr(self.data, func_name)
            pdf = pd_func(*args, **kwargs)
            return DataFrame(pdf, ds=local_ds)
        else:
            df = func(self, *args, **kwargs)
            df.source_table = self.source_table
            df._saved_func_name_ = func_name
            df._saved_args_ = args
            df._saved_kwargs_ = kwargs
            return df
    return inner


def binary_op_local_frame_wrapper(func):
    def inner(self, other):
        func_name = func.__name__
        if self.data is not None:
            if (is_type(other, DataFrame) and other.data is not None) :
            # # todo: check other ds
            # other.materialize()
            # other = other.data
                pd_func = getattr(self.data, func_name)
                pdf = pd_func(other.data)
                return DataFrame(pdf, ds=local_ds)
            elif not is_type(other, DataFrame):
                pd_func = getattr(self.data, func_name)
                pdf = pd_func(other)
                return DataFrame(pdf, ds=local_ds)

        df = func(self, other)
        df.source_table = self.source_table
        df._saved_func_name_ = func_name
        df._saved_args_ = other
        return df
    return inner


class DataFrame:
    tid = 1

    def __init__(self, data=None, table_name=None, transform=None, ds=None, db_persistent=False):
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
        self._db_persistent = db_persistent

        # self.str = None     #return DF
        '''
        check type of the column
        
        '''

    @property
    def str(self):
        columns = self.columns
        is_str = True
        col_type = None

        if not is_str:
            raise ValueError(f"cannot set type {col_type} as str!")
        if len(columns) == 1:
            return self
        raise ValueError(f"operation only supports Series with number of columns = 1!")

    @local_frame_wrapper
    def contains(self, pat: str, case = True, regex = True):
        transform = SQLContainsTransform(self, pat, case, regex)
        return DataFrame(ds=self.data_source, transform=transform)

    @local_frame_wrapper
    def reset_index(self, inplace=True):
        transform = SQLPlaceHolder(self)
        return DataFrame(ds=self.data_source, transform=transform)

    """
    override so that any unsupported function call directly goes to pandas 
    """
    # def __getattr__(self, item):
    #     def dataframe_wrapper(*args, **kwargs):
    #         func_name = item
    #         if self.data is None:
    #             self.materialize()
    #             pd_func = getattr(pd.DataFrame, func_name)
    #             pdf = pd_func(self.data, *args, **kwargs)
    #             return DataFrame(pdf, ds=local_ds)
    #
    #     if item in self.__dict__:
    #         at = dataframe_wrapper
    #     elif item in pd.DataFrame.__dict__:
    #         at = dataframe_wrapper
    #         print('Method/attribute {} is not supported in AIDAC dataframe, '
    #               'using pandas instead'.format(item))
    #     else:
    #         raise AttributeError('Dataframe has no attribute {}'.format(item))
    #     return at

    def __str__(self):
        return self.table_name

    def __setitem__(self, key, value):
        # todo: multiple column
        new_col = next(iter(value.columns))
        trans = SQLProjectionTransform(self, {key: value.new_col})
        tb = DataFrame(transform=trans, ds=self.data_source)
        return tb

    @binary_op_local_frame_wrapper
    def __getitem__(self, key):
        if isinstance(key, DataFrame):
            if not (isinstance(key.transform, SQLFilterTransform) or isinstance(key.transform, SQLContainsTransform)):
                raise ValueError('The given dataframe must be a logical expression obtained from comparison')
            else:
                if self.source_table != key.source_table:
                    print('Select on heterogeneous data')
                else:
                    trans = SQLProjectionTransform([self, key], self.columns.keys())
        elif isinstance(key, list):
            keys = key
            trans = SQLProjectionTransform(self, keys)
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
        return self.source_table if self._db_persistent else self.__tid__

    @property
    def genSQL(self):
        if self.transform is not None:
            return self.transform.genSQL
        else:
            return 'SELECT * FROM ' + self.table_name

    @property
    def genSQL_compat(self):
        try:
            return self.transform.genSQL_compat
        except Exception:
            return self.genSQL

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
                col_idx_name = self.data.columns if hasattr(self.data, 'columns') else [self.data.name]
                col_idx_type = self._data_.dtypes if hasattr(self.data, 'columns') else [self._data_.dtypes]
                for cname, ctype in zip(col_idx_name, col_idx_type):
                    if isinstance(ctype, np.dtype):
                        ctype = ctype.type
                    cols[cname] = Column(cname, ctype)
                self._columns_ = cols
            else:
                if not self._db_persistent:
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

    def merge(self, other, on=None, left_on=None, right_on=None, how='left', suffixes=('_x', '_y'), sort=False):
        # local table, eager execution
        if self.data is not None and other.data is not None:
            rs = self.data.merge(other.data, on=on, how=how, left_on=left_on, right_on=right_on,
                                 suffixes=suffixes, sort=sort)
            tb = DataFrame(data=rs, ds=local_ds)
        else:
            func_name = 'merge'
            saved_args = {'on': on, 'how': how, 'left_on': left_on, 'right_on': right_on,
                          'suffixes': suffixes, 'sort': sort}
            if left_on and right_on:
                trans = SQLJoinTransform(self, other, left_on, right_on, how, suffixes)
            else:
                trans = SQLJoinTransform(self, other, on, on, how, suffixes)

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
    def sort_values(self, orderlist: Union[List[str], str], ascending=True):

        if isinstance(orderlist, str):
            keys = [orderlist]
        else:
            if not orderlist:
                raise ValueError("orderlist cannot be None!")
            keys = orderlist

        transform = SQLOrderTransform(self, keys, ascending)
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
    def agg(self, collist: Union[None, List[str], Dict, str] = None):
        if isinstance(collist, str):
            func = collist
        else:
            func = None
        trans = SQLAGG_Transform(self, func=func, collist=collist)
        return DataFrame(ds=self.data_source, transform=trans)

    def isin(self, values: list):
        # todo: check contents in values
        trans = SQLFilterTransform(self, 'isin', values)

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
    def groupby(self, by: Union[List[str], str], sort=True, axis=0):
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

    @local_frame_wrapper
    def count(self):
        trans = SQLAGG_Transform(self, func='count', collist=self.columns.keys())
        return DataFrame(ds=self.data_source, transform=trans)

    @local_frame_wrapper
    def sum(self):
        trans = SQLAGG_Transform(self, func='sum', collist=self.columns.keys())
        return DataFrame(ds=self.data_source, transform=trans)

    @local_frame_wrapper
    def mean(self):
        trans = SQLAGG_Transform(self, func='avg', collist=self.columns.keys())
        return DataFrame(df=self.data_source, transform=trans)

    @local_frame_wrapper
    def min(self):
        trans = SQLAGG_Transform(self, func='min', collist=self.columns.keys())
        return DataFrame(ds=self.data_source, transform=trans)

    @local_frame_wrapper
    def max(self):
        trans = SQLAGG_Transform(self, func='max', collist=self.columns.keys())
        return DataFrame(ds=self.data_source, transform=trans)

    def to_dict(self, orient, into):
        return self._data_.to_dict(orient, into)

    def to_pickle(self, path, compression='infer', protocol=5, storage_options=None):
        return self._data_.to_pickle(path, compression, protocol, storage_options)

    def to_json(self, path_or_buf=None, orient=None, date_format=None, double_precision=10, force_ascii=True,
                date_unit='ms',
                default_handler=None, lines=False, compression="infer", index=True, indent=None, storage_options=None):
        return self._data_.to_json(path_or_buf, orient, date_format, double_precision, force_ascii, date_unit,
                                   default_handler, lines, compression, index, indent, storage_options)

    @binary_op_local_frame_wrapper
    def isin(self, other):
        if is_type(other, ArrayLike):
            trans = SQLFilterTransform(self, 'in', other)
        else:
            raise TypeError('Currently not support other type isin operation')
        return DataFrame(ds=self.data_source, transform=trans)

    @binary_op_local_frame_wrapper
    def __add__(self, other):
        # add a number to the dataframe
        if isinstance(other, numbers.Number):
            trans = SQLBinaryOperationTransform(self, '+', other, True)
        elif isinstance(other, DataFrame):
            trans = SQLBinaryOperationTransform(self, '+', other)
        else:
            raise TypeError('Addition with {} is not supported'.format(type(other)))
        return DataFrame(ds=self.data_source, transform=trans)

    @binary_op_local_frame_wrapper
    def __sub__(self, other):
        # add a number to the dataframe
        if isinstance(other, numbers.Number):
            trans = SQLBinaryOperationTransform(self, '-', other, True)
        elif isinstance(other, DataFrame):
            trans = SQLBinaryOperationTransform(self, '-', other)
        else:
            raise TypeError('Subtraction with {} is not supported'.format(type(other)))
        return DataFrame(ds=self.data_source, transform=trans)

    @binary_op_local_frame_wrapper
    #todo: handle carefully
    def __rsub__(self, other):
        if isinstance(other, numbers.Number):
            trans = SQLBinaryOperationTransform(self, '-', other, True)
        else:
            raise TypeError('Subtraction with {} is not supported'.format(type(other)))
        return DataFrame(ds=self.data_source, transform=trans)

    @binary_op_local_frame_wrapper
    def __mul__(self, other):
        if isinstance(other, numbers.Number):
            trans = SQLBinaryOperationTransform(self, '*', other, True)
        elif isinstance(other, DataFrame):
            trans = SQLBinaryOperationTransform(self, '*', other)
        else:
            raise TypeError('Multiplication with {} is not supported'.format(type(other)))
        return DataFrame(ds=self.data_source, transform=trans)

    @binary_op_local_frame_wrapper
    def __truediv__(self, other):
        if isinstance(other, numbers.Number):
            trans = SQLBinaryOperationTransform(self, '/', other, True)
        elif isinstance(other, DataFrame):
            trans = SQLBinaryOperationTransform(self, '/', other)
        else:
            raise TypeError('True division with {} is not supported'.format(type(other)))
        return DataFrame(ds=self.data_source, transform=trans)

    @local_frame_wrapper
    def __eq__(self, other):
        if is_type(other, ConstantTypes) or is_type(other, DataFrame):
            trans = SQLFilterTransform(self, "eq", other)
            return DataFrame(ds=self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __ge__(self, other):
        if is_type(other, ConstantTypes) or is_type(other, DataFrame):
            trans = SQLFilterTransform(self, "ge", other)
            return DataFrame(ds=self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __gt__(self, other) :
        if is_type(other, ConstantTypes) or is_type(other, DataFrame):
            trans = SQLFilterTransform(self, "gt", other)
            return DataFrame(ds=self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __ne__(self, other):
        if is_type(other, ConstantTypes) or is_type(other, DataFrame):
            trans = SQLFilterTransform(self, "ne", other)
            return DataFrame(ds=self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __lt__(self, other) :
        if is_type(other, ConstantTypes) or is_type(other, DataFrame):
            trans = SQLFilterTransform(self, "lt", other)
            return DataFrame(ds=self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @local_frame_wrapper
    def __le__(self, other):
        if is_type(other, ConstantTypes) :
            trans = SQLFilterTransform(self, "le", other)
            return DataFrame(ds=self.data_source, transform=trans)
        raise ValueError("object comparison is not supported by remotetables")

    @binary_op_local_frame_wrapper
    def __and__(self, other):
        if isinstance(other, DataFrame) and isinstance(other.transform, SQLFilterTransform):
            trans = SQLFilterTransform(self, 'AND', other)
            return DataFrame(ds=self.data_source, transform=trans)

    @binary_op_local_frame_wrapper
    def __or__(self, other):
        if isinstance(other, DataFrame) and isinstance(other.transform, SQLFilterTransform):
            trans = SQLFilterTransform(self, 'OR', other)
            return DataFrame(ds=self.data_source, transform=trans)

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
        return self.source_table if self._db_persistent else self.__tid__

    @classmethod
    def read_orc(cls, path, columns=None, **kwargs):
        return DataFrame(pd.read_orc(columns, **kwargs))

    #######################################
    #
    # functions to be used only by the wrapper
    #
    #######################################

    def _validate_input_column_size(self, key, val):
        # validate the type of the input
        if not (is_type(val, ArrayLike) or is_type(val, DataFrame) or is_type(val, ConstantTypes)):
            raise TypeError("Assigning unsupported type to DataFrame column")

        # validate the size of the input
        if is_type(key, ArrayLike):
            # todo: validate row size
            # multiple key, must match multiple columns
            # if dataframe's column num/ array column num does not match the key num
            if (is_type(val, DataFrame) and len(key) != len(val.columns)) \
                    or (is_type(val, ArrayLike) and len(key) != np.shape(val)[-1]):
                    raise ValueError('Columns must have the same length as the keys')
        else:
            if is_type(val, DataFrame) and len(val.columns) != 1:
                raise ValueError('Assigned Dataframe has different column length')

    def _handle_df_cols(self, key, val):
        # todo: self source table is product of previous merge
        one_table = True

        col = val
        if col.source_table != self.source_table:
            warnings.warn('Assigned column(s) from a different table. Data will be handled locally')
            one_table = False

        new_col = None

        if one_table:
            new_col = Column(key, col.dtype, table=col.tablename, transform=col.column_expr)

        return new_col

    def _format_column(self, key, val):
        if is_type(val, ConstantTypes):
            new_col = Column(key, type(val))
            if is_type(val, NumericTypes):
                new_col.column_expr = str(val)
            else:
                new_col.column_expr = '\''+val+'\''
            return new_col

    def _handle_assign_locally(self, key, val):
        self.materialize()
        vs = []
        if is_type(key, ArrayLike):
            for k, v in zip(key, val):
                if hasattr(v,'materialize'):
                    vs.append(v.materialize())
                else:
                    vs.append(v)
        else:
            if hasattr(val, 'materialize'):
                vs = val.materialize()

        self.data[key] = vs
        return DataFrame(data=self.data, ds=local_ds)

    def project(self, key, val):
        """
        @param key: column name
        @param val: dataframe
        @return:
        """
        # If data is local, let pandas handle the rest
        # When data is remote, check if column length matches.
        # Then check if the columns are from the same table or constant, if so, then rewrite and store the sql query
        # Otherwise check if the row size matches using estimation of both source data and value to be assigned

        # There is no corresponding pd function, have to invoke local pandas function manually
        if self.data is not None:
            pdf = self.data
            if isinstance(val, DataFrame):
                val = val.data
            pdf[key] = val
            return DataFrame(data=pdf, ds=local_ds)

        # validate the input having the correct size
        self._validate_input_column_size(key, val)

        all_cols = self.columns

        # check the type of val and create corresponding column object
        if is_type(key, ArrayLike):
            if not is_type(val, DataFrame):
                for k, v in zip(key, val):
                    const_col = self._format_column(k, v)
                    if const_col:
                        all_cols[k] = const_col
            else:
                # check the key and the df have the same column length
                if len(key) != len(val.columns):
                    raise ValueError('The data to be assigned must have the same dimension as the keys')
                else:
                    for k, v in zip(key, val.columns):
                        new_col = self._handle_df_cols(k, v)
                        if new_col:
                            all_cols[k] = new_col
                        else:
                            return self._handle_assign_locally(key, val)
        else:
            # we only allow 1 column to be assigned to a new column for now
            if len(val.columns) != 1:
                raise ValueError('Can only assign dim-1 vector to a column')

            new_col = self._handle_df_cols(key, list(val.columns.values())[0])
            if new_col:
                all_cols[key] = new_col
            else:
                return self._handle_assign_locally(key, val)

        trans = SQLProjectionTransform(self, all_cols)
        tb = DataFrame(transform=trans, ds=self.data_source)
        return tb


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



from __future__ import annotations

from ctypes import Union

import pandas as pd
from typing import List, Dict

from aidac.data_source.DataSource import local_ds
from aidac.dataframe.frame import DataFrame


def create_remote_table(source, table_name):
    return WFrame(DataFrame(ds=source, table_name=table_name, db_persistent=True))


def _extract_frame(df):
    if isinstance(df, WFrame):
        return df._tail_frame
    return df


class WFrame:
    def __init__(self, df):
        self._frame_stubs = [df]
        self._tail_frame = df

    def __getitem__(self, key):
        if isinstance(key, WFrame):
            key = key._tail_frame
        return WFrame(self._tail_frame[key])

    def __setitem__(self, key, value):
        # todo: put the logic here or in dataframe?
        # create a new dataframe internally and update the stub list
        if isinstance(value, WFrame):
            value = value._tail_frame
        new_df = self._tail_frame.project(key, value)
        self._frame_stubs.append(new_df)
        self._tail_frame = new_df

    def materialize(self):
        self._tail_frame.materialize()
        if len(self._frame_stubs) > 1:
            for df in self._frame_stubs[:-1]:
                if df.data is None:
                    df._data_ = self._tail_frame[df.columns.keys()]
                    df.clear_lineage()

    #####################################33
    # Dataframe method wrapper
    ########################################33
    
    def __str__(self):
        return str(self._tail_frame)

    def __getitem__(self, key):
        if isinstance(key, WFrame):
            key = key._tail_frame
        return WFrame(self._tail_frame[key])

    @property
    def data(self):
        return self._tail_frame.data

    @property
    def id(self):
        return self._tail_frame.id

    @property
    def table_name(self):
        return self._tail_frame.table_name

    @property
    def genSQL(self):
        return self._tail_frame.genSQL

    @property
    def columns(self):
        return self._tail_frame.columns

    @property
    def data_source(self):
        return self._tail_frame.data_source

    @property
    def transform(self):
        return self._tail_frame.transform

    @property
    def str(self):
        return self._tail_frame.str

    def __repr__(self) -> str:
        """
        @return: string representation of current dataframe
        """
        pass

    def merge(self, other: WFrame, on=None, left_on=None, right_on=None, how='inner', suffixes=('_x', '_y'), sort=False):
        df = self._tail_frame.merge(other._tail_frame, on=on, left_on=left_on, right_on=right_on,
                                    how=how, suffixes=suffixes, sort=sort)
        return WFrame(df)

    def fillna(self, col=[], val=0):
        return WFrame(self._tail_frame.fillna(col=col, val=val))

    def dropna(self, col=[]):
        return WFrame(self._tail_frame.dropna(col=col))

    def drop_duplicates(self):
        return WFrame(self._tail_frame.drop_duplicates())

    def query(self, expr: str):
        return WFrame(self._tail_frame.query(expr=expr))

    def apply(self, func, axis=0):
        return WFrame(self._tail_frame.apply(func, axis=axis))

    def aggregate(self, projcols, groupcols=None):
        return WFrame(self._tail_frame.aggregate(projcols, groupcols=groupcols))

    def agg(self, collist=None):
        return WFrame(self._tail_frame.agg(collist))

    def contains(self, pat: str, case=True, regex=True):
        return WFrame(self._tail_frame.contains(pat=pat, case=case, regex=regex))

    def head(self, n=5):
        return WFrame(self._tail_frame.head(n))

    def tail(self, n=5):
        return WFrame(self._tail_frame.tail(n))

    def rename(self, columns: Dict):
        # todo: maybe need the wrapper to do it
        return WFrame(self._tail_frame.rename(columns))

    def groupby(self, by: Union[List[str], str], sort=True, axis=0):
        return WFrame(self._tail_frame.groupby(by, sort=sort, axis=axis))

    def sort_values(self,  orderlist: Union[List[str], str], ascending=True):
        return WFrame(self._tail_frame.sort_values(orderlist, ascending=ascending))

    def reset_index(self, inplace=True):
        return WFrame(self._tail_frame.reset_index(inplace=inplace))

    def count(self):
        return WFrame(self._tail_frame.count())

    def sum(self):
        return WFrame(self._tail_frame.sum())

    def min(self):
        return WFrame(self._tail_frame.min())

    def max(self):
        return WFrame(self._tail_frame.max())

    def to_dict(self, orient, into):
        return self._tail_frame.to_dict(orient, into)

    def to_pickle(self, path, compression='infer', protocol=5, storage_options=None):
        return self._tail_frame.to_pickle(path, compression, protocol, storage_options)

    def to_json(self, path_or_buf=None, orient=None, date_format=None, double_precision=10, force_ascii=True,
                date_unit='ms',
                default_handler=None, lines=False, compression="infer", index=True, indent=None, storage_options=None):
        return self._tail_frame.to_json(path_or_buf, orient, date_format, double_precision, force_ascii, date_unit,
                                        default_handler, lines, compression, index, indent, storage_options)

    def isin(self, other):
        return WFrame(self._tail_frame.isin(_extract_frame(other)))

    def __add__(self, other):
        return WFrame(self._tail_frame + _extract_frame(other))

    def __radd__(self, other):
        return WFrame(other + self._tail_frame)

    def __mul__(self, other):
        other = _extract_frame(other)
        return WFrame(self._tail_frame * other)

    def __rmul__(self, other):
        return WFrame(other - self._tail_frame)

    def __rsub__(self, other):
        return WFrame(other - self._tail_frame)

    def __sub__(self, other):
        other = _extract_frame(other)
        return WFrame(self._tail_frame - other)

    def __truediv__(self, other):
        other = _extract_frame(other)
        return WFrame(self._tail_frame / other)

    def __eq__(self, other):
        if isinstance(other, WFrame):
            other = other._tail_frame
        return WFrame(self._tail_frame == other)

    def __ge__(self, other):
        if isinstance(other, WFrame):
            other = other._tail_frame
        return WFrame(self._tail_frame >= other)

    def __gt__(self, other):
        if isinstance(other, WFrame):
            other = other._tail_frame
        return WFrame(self._tail_frame > other)

    def __ne__(self, other):
        if isinstance(other, WFrame):
            other = other._tail_frame
        return WFrame(self._tail_frame != other)

    def __lt__(self, other):
        if isinstance(other, WFrame):
            other = other._tail_frame
        return WFrame(self._tail_frame < other)

    def __le__(self, other):
        if isinstance(other, WFrame):
            other = other._tail_frame
        return WFrame(self._tail_frame <= other)

    def __and__(self, other):
        if isinstance(other, WFrame):
            other = other._tail_frame
        return WFrame(self._tail_frame & other)

    def __or__(self, other):
        if isinstance(other, WFrame):
            other = other._tail_frame
        return WFrame(self._tail_frame | other)

    def to_string(self, buf=None, columns=None, col_space=None, header=True, index=True, na_rep='NaN', formatters=None,
                  float_format=None, sparsify=None, index_names=True, justify=None, max_rows=None, max_cols=None,
                  show_dimensions=False, decimal='.', line_width=None, min_rows=None, max_colwidth=None, encoding=None):
        return self._tail_frame.to_string(buf, columns, col_space, header, index, na_rep, formatters, float_format, sparsify,
                                     index_names, justify, max_rows, max_cols, show_dimensions, decimal, line_width,
                                     min_rows, max_colwidth, encoding)

    def to_csv(self, path_or_buf=None, sep=',', na_rep='', float_format=None, columns=None, header=True, index=True,
               index_label=None, mode='w', encoding=None, compression='infer', quoting=None, quotechar='"',
               line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal='.',
               errors='strict', storage_options=None):
        return self._tail_frame.to_csv(path_or_buf, sep, na_rep, float_format, columns, header, index, index_label, mode,
                                  encoding, compression, quoting, quotechar, line_terminator, chunksize, date_format,
                                  doublequote, escapechar, decimal, errors, storage_options)

    @classmethod
    def read_pickle(cls, filepath_or_buffer, compression, dict__, storage_options):
        return WFrame(DataFrame(pd.read_pickle(filepath_or_buffer, compression, dict__, storage_options)))

    @classmethod
    def read_orc(cls, path, columns=None, **kwargs):
        return WFrame(DataFrame(pd.read_orc(columns, **kwargs)))


def read_csv(path, delimiter, header, names, parse_dates=False) -> DataFrame:
    df = pd.read_csv(path, delimiter=delimiter, header=header, names=names, parse_dates=parse_dates)
    cols = df.columns
    return WFrame(DataFrame(data=df, ds=local_ds))


def from_dict(data):
    df = pd.DataFrame(data)
    return WFrame(DataFrame(data=df, ds=local_ds))

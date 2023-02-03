import collections
from collections.abc import Iterable

import numpy as np

type_map = {
    "date": "datetime64[D]",
    np.datetime64: "datetime64[D]"
}

class ResultSet:
    def __init__(self, cols: Iterable, data: Iterable):
        self.columns = cols
        self.data = data

    def get_result_ls(self):
        """
        Get only data in list
        @return: records in flattened list
        """
        return self._flatten(self.data)

    def get_value(self):
        """
        get the first result tuple, return a single value if there is only 1 col and 1 row
        @return:
        """
        rs = self.data
        if self.data:
            rs = self.data[0] if len(self.data[0]) > 1 else self.data[0][0]
        return rs

    def _format2pd(self):
        pass

    def _flatten(self, nested):
        """
        Only consider the first element of each result tuple
        return a flattened list
        @param nested: a list of tuples
        @return:
        """
        rs = [x[0] for x in nested]
        return rs

    def get_result_table(self):
        od = collections.OrderedDict()

        def _gen_type(data, row, col):

            row_max = len(data)
            col_max = len(data[0])
            tp = type(data[row][col])

            while tp == type(None) and row < row_max:
                row += 1
                tp = type(data[row][col])

            return tp

        if not self.data:
            return od

        for idx1, col in enumerate(self.columns):
            # tp = type(self.data[0][idx1])
            tp = type(self.data[0][idx1])
            if tp == str:
                tp = object  #

            od[col.name] = np.empty(len(self.data), dtype=tp)
            for idx2, row in enumerate(self.data):
                try:
                    od[col.name][idx2] = row[idx1]
                except (ValueError, TypeError):
                    # print(f'column: {col}, row_value={row[idx1]}')
                    pass

        row_max = len(self.data)
        col_max = len(self.columns)
        # print(f'returned table size: {row_max*col_max}')
        return od

    def to_tb(self, tracked_cols):
        od = collections.OrderedDict()

        if not self.data:
            return od

        # todo: got datetime, need datetime64 (from tracked_cols)?
        for idx1, col_name in enumerate(tracked_cols):
            # tp = type(self.data[0][idx1])
            col = tracked_cols[col_name]
            tp = col.dtype if col.dtype not in type_map else type_map[col.dtype]

            od[col.name] = np.empty(len(self.data), dtype=tp)
            for idx2, row in enumerate(self.data):
                try:
                    od[col.name][idx2] = row[idx1]
                except (ValueError, TypeError):
                    # print(f'column: {col}, row_value={row[idx1]}')
                    pass

        row_max = len(self.data)
        col_max = len(self.columns)
        # print(f'returned table size: {row_max}*{col_max}')
        return od

    def to_pd(self):
        """
        input: columns, data,
        (call get_result_table)

        output:
        pandas dataframe

        convert date time into compatible format

        convert data and column to pandas
        @return:
        """
        for idx1, col in enumerate(self.columns):
            tp = type(self.data[0][idx1])

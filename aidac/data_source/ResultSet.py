import collections
from collections.abc import Iterable

import numpy as np
from convert import convert

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

        for idx1, col in enumerate(self.columns):
            # tp = type(self.data[0][idx1])
            tp = _gen_type(self.data, 0, idx1)
            if tp == str:
                tp = object  #

            od[col.name] = np.empty(len(self.data), dtype=tp)
            for idx2, row in enumerate(self.data):
                try:
                    od[col.name][idx2] = row[idx1]
                except (ValueError, TypeError):
                    # print(f'column: {col}, row_value={row[idx1]}')
                    pass
        return od
# from convert import convert
# try:
#     dummy = [{'col1': 2}, {'col1': 'None'}]
#     cv = convert(dummy, ['col1'], 2, 1)
# except Exception as e:
#     print(e)
# print('cv: '+str(cv))

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


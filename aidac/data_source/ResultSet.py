import collections
from collections.abc import Iterable

import numpy as np


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
        rs = self.data[0][0]
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
        for idx1, col in enumerate(self.columns):
            tp = type(self.data[0][idx1])
            if tp == str:
                tp = object
            od[col.name] = np.empty(len(self.data), dtype=tp)
            for idx2, row in enumerate(self.data):
                od[col.name][idx2] = row[idx1]
        return od


    def to_pd(self):
        """
        convert data and column to pandas
        @return:
        """


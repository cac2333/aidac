import datetime

import numpy as np

SIZE_INT = 4
SIZE_NUM, SIZE_TIME = 4, 8
SIZE_OBJ = 50

sizes = { np.int8: SIZE_INT,
          np.int16: SIZE_INT, np.int32: SIZE_INT, np.int64: SIZE_NUM, float: SIZE_NUM
    , np.float32: SIZE_NUM, np.float64: SIZE_NUM,
          np.object: SIZE_OBJ,
          np.object_: SIZE_OBJ, bytearray: SIZE_OBJ
    , datetime.date: SIZE_TIME, datetime.time: SIZE_TIME, 'timestamp': SIZE_TIME, np.datetime64: SIZE_TIME}

class Column:
    def __init__(self, name=None, dtype=None, table=None, schema=None, nullable=True, srccol=None, transform=None,
                 source_table=None, avg_size=-1, agg_func=None, null_frac=0, max_val=None, n_distinct=1):
        self.name = name
        self.dtype = dtype
        self.tablename = table
        self.schema = schema
        self.nullable = nullable
        self.srccol = srccol
        self.column_expr = transform
        self.source_table = source_table
        self.agg_func = agg_func
        self.avg_size = avg_size
        self.null_frac = null_frac
        self.max_val = max_val
        self.n_distinct = n_distinct

    def full_name(self):
        # return self.tablename[0] + '.' +self.name
        return self.name

    def get_size(self):
        if self.avg_size is not None and self.avg_size >0:
            return self.avg_size
        else:
            if self.dtype and self.dtype in sizes:
                return sizes[self.dtype]
            return 4

    def __str__(self):
        str = f'Column {self.name}: <type={self.dtype},' \
              f' table={self.tablename}, srccol={self.srccol}, ' \
              f'db_table={self.source_table}, agg={self.agg_func}, expr={self.column_expr}>'
        return str

def get_size(tp):
    if tp and tp in sizes:
        return sizes[tp]
    return 4


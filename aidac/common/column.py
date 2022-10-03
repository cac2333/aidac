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
    def __init__(self, name, dtype, table=None, schema=None, nullable=True, srccol=None, transform=None, source_table=None, agg_func=None):
        self.name = name
        self.dtype = dtype
        self.tablename = table
        self.schema = schema
        self.nullable = nullable
        self.srccol = srccol
        self.column_expr = transform
        self.source_table = source_table
        self.agg_func = agg_func

    def full_name(self):
        # return self.tablename[0] + '.' +self.name
        return self.name

    def get_size(self):
        if self.dtype and self.dtype in sizes:
            return sizes[self.dtype]
        return 4

def get_size(tp):
    if tp and tp in sizes:
        return sizes[tp]
    return 4


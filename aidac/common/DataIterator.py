import numpy as np


def generator(data):
    for row in data.to_numpy():
        if row.dtype == np.int32 or np.int64:
            row = row.astype(str)
        yield row


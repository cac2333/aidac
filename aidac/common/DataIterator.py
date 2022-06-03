import numpy as np


def generator(data):
    rs = []
    for row in data.to_numpy():
        if row.dtype == np.int32:
            row = row.astype(str)
        rs.append(tuple(row))
    return rs


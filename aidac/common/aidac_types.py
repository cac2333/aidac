import datetime
from typing import Union

import numpy as np
import typing

NumericTypes = Union[float, int, np.int, np.float, np.float32, np.float64, np.int32]
FloatType = Union[float, np.float, np.float64, np.float32]
TimeTypes = Union[datetime.time, datetime.date, datetime.datetime, np.datetime64]
# ConstantTypes = Union[str, float, int, bool,
#                       np.int, np.float,
#                       datetime.date, datetime.datetime, datetime.timedelta, np.datetime64]
ConstantTypes = Union[NumericTypes, FloatType, TimeTypes, str]
ArrayLike = Union[np.ndarray, list, tuple]



def in_type(value, expected: typing._GenericAlias) -> bool:
    return value in expected.__args__


def is_type(value, expected) -> bool:
    if isinstance(expected, typing._GenericAlias):
        return isinstance(value, expected.__args__)
    else:
        return isinstance(value, expected)
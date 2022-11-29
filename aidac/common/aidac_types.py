import datetime
from typing import Union

import numpy as np
import typing

ConstantTypes = Union[str, float, int, bool, np.int, np.float, datetime.date, datetime.datetime, datetime.timedelta, np.datetime64]
ArrayLike = Union[np.ndarray, list, tuple]
NumericTypes = Union[float, int, np.int, np.float, np.float32, np.float64, np.int32]
FloatType = Union[float, np.float, np.float64, np.float32]


def in_type(value, expected: typing._GenericAlias) -> bool:
    return value in expected.__args__


def is_type(value, expected) -> bool:
    if isinstance(expected, typing._GenericAlias):
        return isinstance(value, expected.__args__)
    else:
        return isinstance(value, expected)
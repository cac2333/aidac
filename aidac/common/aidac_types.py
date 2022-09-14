import datetime
from typing import Union

import numpy as np
import typing

ConstantTypes = Union[str, float, int, bool, np.int, np.float, datetime.date, datetime.datetime, datetime.timedelta]
ArrayLike = Union[np.ndarray, list, tuple]
NumericTypes = Union[float, int, np.int, np.float]


def is_type(value, expected) -> bool:
    if isinstance(expected, typing._GenericAlias):
        return isinstance(value, expected.__args__)
    else:
        return isinstance(value, expected)
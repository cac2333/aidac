import unittest
from aidac.dataframe.frame_wrapper import *


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dummy_dict1 = {
            'id': [1, 2, 3, 4, 5, 6],
            'float': [1.1, 2.2, 3.3, 4.4, 5.5, 6.6],
            'str': ['str1', 'str2', 'str3', 'str4', 'str5', 'str6']
        }
        self.dummy_dict2 = {
            'id': [3, 4, 4, 6],
            'value': ['this', 'is', 'a', 'string']
        }

    def test_wrapper_normal_functions(self):
        base1 = from_dict(self.dummy_dict1)
        filter = base1.query('float > 3')
        proj = filter[['id', 'str']]
        jn = from_dict(self.dummy_dict2).merge(proj, on='id')
        jn.materialize()


if __name__ == '__main__':
    unittest.main()

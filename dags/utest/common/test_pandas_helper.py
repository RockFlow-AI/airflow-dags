import unittest

from rockflow.common.pandas_helper import *
from pandas.testing import assert_frame_equal
import numpy as np


class TestYahoo(unittest.TestCase):
    def test_merge_data_frame_by_column(self):
        df_1 = pd.DataFrame(
            data={
                'col1': [1, 2],
                'col2': [3, 4]
            })
        df_2 = pd.DataFrame(
            data={
                'col1': [5, 6],
                'col3': [7, 8]
            })
        df_3 = pd.DataFrame(
            data={
                'col1': [1, 2, 5, 6],
                'col2': [3, 4, np.nan, np.nan],
                'col3': [np.nan, np.nan, 7, 8]
            })
        df_list = [df_1, df_2]
        result_1 = merge_data_frame_by_column(df_list)
        print(result_1)
        assert_frame_equal(result_1, df_3)

    def test_merge_data_frame_by_index(self):
        df_1 = pd.DataFrame.from_dict(
            {
                'col1': {1: 1}
            })
        df_2 = pd.DataFrame.from_dict(
            {
                'col2': {2: 2}
            })
        df_3 = pd.DataFrame.from_dict(
            {
                'col1': {1: 1, 2: np.nan},
                'col2': {1: np.nan, 2: 2}
            })
        df_list = [df_1, df_2]
        result_2 = merge_data_frame_by_index(df_list)
        print(result_2)
        assert_frame_equal(result_2, df_3)


if __name__ == '__main__':
    unittest.main()

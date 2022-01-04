import unittest

import numpy as np
import pandas as pd
from airflow.exceptions import AirflowException
from rockflow.operators.symbol import MergeCsvList


class TestYahoo(unittest.TestCase):
    def test_nan(self):
        df_1 = pd.DataFrame.from_dict(
            {
                'col1': {1: 1, 2: np.nan},
                'col2': {1: np.nan, 2: 2}
            })
        df_2 = pd.DataFrame.from_dict(
            {
                'col1': {1: 1, 2: 2},
                'col2': {1: 1, 2: 2}
            })
        self.assertTrue(df_1.isna().any().any())
        self.assertFalse(df_2.isna().any().any())

    def test_merge_symbol(self):
        merge = MergeCsvList(
            from_key="symbol_download_parse",
            key="symbol_download_merge"
        )
        self.assertRaises(AirflowException, lambda: merge.execute(""))


if __name__ == '__main__':
    unittest.main()

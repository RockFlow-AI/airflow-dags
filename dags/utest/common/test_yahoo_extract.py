import unittest

from rockflow.dags.const import *
from rockflow.operators.yahoo import YahooExtractOperator


class TestYahoo(unittest.TestCase):
    def test_yahoo(self):
        yahoo = YahooExtractOperator(
            from_key="yahoo_download_debug_yahoo",
            key="yahoo_extract",
            region=DEFAULT_REGION,
            bucket_name=DEFAULT_BUCKET_NAME
        )
        self.assertIsNotNone(yahoo.execute(""))

    def test_merge(self):
        test_data1 = [
            [1, {"a": "a1", "b": "b1"}],
            [2, {"a": "a2", "b": "b2"}],
            [3, {"a": "a3", "b": "b3"}],
            [4, {"a": "a4", "b": "b4"}]
        ]
        test_data2 = [
            [1, {"a": "a1", "b": "b1"}],
            [1, {"a": "a2", "b": "b2"}],
            [3, {"a": "a3", "b": "b3"}],
            [4, {"a": "a4", "b": "b4"}]
        ]
        self.assertEqual(
            len(YahooExtractOperator.merge_data(test_data1)),2
        )
        self.assertNotEqual(
            YahooExtractOperator.merge_data(test_data2)["a"][1],"a2"
        )


if __name__ == '__main__':
    unittest.main()

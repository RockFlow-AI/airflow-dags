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


if __name__ == '__main__':
    unittest.main()

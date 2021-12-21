import unittest

from rockflow.dags.const import *
from rockflow.operators.yahoo import YahooExtractAssetProfileOperator


class TestYahoo(unittest.TestCase):
    def test_yahoo(self):
        yahoo = YahooExtractAssetProfileOperator(
            from_key="yahoo_download_debug_yahoo",
            key="yahoo_assert_profile",
            region=DEFAULT_REGION,
            bucket_name=DEFAULT_BUCKET_NAME
        )
        self.assertIsNotNone(yahoo.execute(""))


if __name__ == '__main__':
    unittest.main()

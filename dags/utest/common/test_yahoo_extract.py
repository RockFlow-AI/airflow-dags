import unittest

from rockflow.dags.const import *
from rockflow.dags.symbol import yahoo_extract_us, yahoo_extract_none_us, summary_detail_mysql_us, \
    summary_detail_mysql_none_us
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

    def test_yahoo_extract_us(self):
        self.assertIsNone(yahoo_extract_us.execute(""))

    def test_yahoo_extract_none_us(self):
        self.assertIsNone(yahoo_extract_none_us.execute(""))

    def test_summary_detail_mysql_us(self):
        self.assertIsNone(summary_detail_mysql_us.execute(""))

    def test_summary_detail_mysql_none_us(self):
        self.assertIsNone(summary_detail_mysql_none_us.execute(""))


if __name__ == '__main__':
    unittest.main()

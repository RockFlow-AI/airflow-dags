import unittest

from rockflow.dags.symbol import yahoo_extract_us, yahoo_extract_none_us, summary_detail_mysql_us, \
    summary_detail_mysql_none_us, yahoo


class TestYahoo(unittest.TestCase):
    def test_yahoo_extract_us(self):
        self.assertIsNone(yahoo_extract_us.execute(""))

    def test_yahoo_extract_none_us(self):
        self.assertIsNone(yahoo_extract_none_us.execute(""))

    def test_summary_detail_mysql_us(self):
        self.assertIsNone(summary_detail_mysql_us.execute(""))

    def test_summary_detail_mysql_none_us(self):
        self.assertIsNone(summary_detail_mysql_none_us.execute(""))

    def test_yahoo(self):
        self.assertIsNone(yahoo.execute(""))


if __name__ == '__main__':
    unittest.main()

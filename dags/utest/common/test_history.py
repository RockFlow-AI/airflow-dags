import unittest

from rockflow.dags.history import *
from rockflow.operators.history import date_to_timestamp, get_symbol


class Test(unittest.TestCase):
    def test_market(self):
        self.assertIsNotNone(hkex_sync_debug.execute(""))
        self.assertIsNone(us_sync_debug.execute(""))

    def test_date_to_timestamp(self):
        tests = {
            "2015-01-02": 1420128000000,
            "2015-01-03": 1420214400000,
        }
        for k, v in tests.items():
            self.assertEqual(date_to_timestamp(k), v)

    def test_get_symbol(self):
        tests = {
            "00002-9988.HK-阿里巴巴-SW.csv": "09988.HK",
            "00012-1288.HK-农业银行.csv": "01288.HK",
            "00011-TCTZF-TENCENT HOLDINGS LIMITED.csv": "TCTZF",
            "00014-JPM-摩根大通.csv": "JPM",
        }
        for k, v in tests.items():
            self.assertEqual(get_symbol(k), v)


if __name__ == '__main__':
    unittest.main()

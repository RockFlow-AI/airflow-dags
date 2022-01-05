import unittest

from rockflow.dags_test.once import symbol_download_futu_company_profile_cn, symbol_download_futu_company_profile_en, \
    symbol_download_yahoo


class Test(unittest.TestCase):
    def test_symbol_download_futu_company_profile_cn(self):
        self.assertIsNone(symbol_download_futu_company_profile_cn.execute(""))

    def test_symbol_download_futu_company_profile_en(self):
        self.assertIsNone(symbol_download_futu_company_profile_en.execute(""))

    def test_symbol_download_yahoo(self):
        self.assertIsNone(symbol_download_yahoo.execute(""))


if __name__ == '__main__':
    unittest.main()

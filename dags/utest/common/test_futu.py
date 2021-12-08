import unittest

from rockflow.common.futu_company_profile import FutuCompanyProfileCn, FutuCompanyProfileEn


class TestNasdaq(unittest.TestCase):
    # python -m unittest test_futu.TestNasdaq.test_nasdaq
    def test_nasdaq(self):
        ticker_list = [
            ("AAPL-US", "AAPL-US"),
            ("00700-HK", "00700-HK"),
            ("000001-SZ", "000001-SZ"),
            ("000002-SZ", "000002-SZ"),
            ("600000-SH", "600000-SH"),
            ("600011-SH", "600011-SH"),
        ]

        for ticker in ticker_list:
            cn_obj = FutuCompanyProfileCn(ticker[0], ticker[1])
            cn_obj.to_json(cn_obj.get().content)
            en_ojb = FutuCompanyProfileEn(ticker[0], ticker[1])
            en_ojb.to_json(en_ojb.get().content)


if __name__ == '__main__':
    unittest.main()

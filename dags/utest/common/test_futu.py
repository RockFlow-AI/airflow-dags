import unittest

from rockflow.common.futu_company_profile import FutuCompanyProfileCn, FutuCompanyProfileEn
from rockflow.common.proxy import local_proxy


class Test(unittest.TestCase):
    def test(self):
        ticker_list = [
            ("AAPL-US", "AAPL-US"),
            ("00700-HK", "00700-HK"),
            ("000001-SZ", "000001-SZ"),
            ("000002-SZ", "000002-SZ"),
            ("600000-SH", "600000-SH"),
            ("600011-SH", "600011-SH"),
        ]
        prefix = ""
        for ticker in ticker_list:
            cn_obj = FutuCompanyProfileCn(
                ticker[0], ticker[1], prefix, proxy=local_proxy())
            self.assertIsNotNone(cn_obj.to_json(cn_obj.get().content))
            en_ojb = FutuCompanyProfileEn(
                ticker[0], ticker[1], prefix, proxy=local_proxy())
            self.assertIsNotNone(en_ojb.to_json(en_ojb.get().content))


if __name__ == '__main__':
    unittest.main()

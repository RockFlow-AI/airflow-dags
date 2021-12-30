import unittest

from rockflow.common.futu_company_profile import FutuCompanyProfileCn, FutuCompanyProfileEn
from rockflow.common.proxy import local_proxy
from rockflow.dags.const import DEFAULT_REGION, DEFAULT_BUCKET_NAME, DEFAULT_PROXY
from rockflow.dags.symbol import MERGE_CSV_KEY, sink_futu_profile_op
from rockflow.operators.futu import JoinMap


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

    def test_join_map(self):
        join_map = JoinMap(
            first="company_profile_batch_download_debug_futu_format_json_en_futu_company_profile_en/futu_format_json_en.json",
            second="company_profile_batch_download_debug_futu_format_json_cn_futu_company_profile_cn/futu_format_json_cn.json",
            merge_key=MERGE_CSV_KEY,
            key="company_profile_batch_download",
            region=DEFAULT_REGION,
            bucket_name=DEFAULT_BUCKET_NAME,
            proxy=DEFAULT_PROXY
        )
        print(join_map.content)
        self.assertIsNotNone(join_map.content)

    def test_sink_futu_profile_op(self):
        self.assertIsNone(sink_futu_profile_op.execute(""))


if __name__ == '__main__':
    unittest.main()

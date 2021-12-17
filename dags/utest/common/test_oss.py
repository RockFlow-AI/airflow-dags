import unittest

from rockflow.operators.futu import FutuBatchOperatorCnDebug


class Test(unittest.TestCase):
    def test(self):
        t = FutuBatchOperatorCnDebug(
            from_key="symbol_download_merge/merge.csv",
            key="test_oss",
            region="cn-hongkong",
            bucket_name="rockflow-data-dev",
            oss_conn_id="oss_default",
        )
        t.execute("")


if __name__ == '__main__':
    unittest.main()

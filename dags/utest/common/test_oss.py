import unittest

from rockflow.dags.const import *
from rockflow.operators.futu import FutuBatchOperatorCnDebug


class Test(unittest.TestCase):
    def test(self):
        t = FutuBatchOperatorCnDebug(
            from_key="symbol_download_merge/merge.csv",
            key="test_oss",
            oss_conn_id="oss_default",
            region=DEFAULT_REGION,
            bucket_name=DEFAULT_BUCKET_NAME
        )
        t.execute("")


if __name__ == '__main__':
    unittest.main()

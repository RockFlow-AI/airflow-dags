import unittest

from rockflow.operators.futu import FutuBatchOperatorCnDebug


class Test(unittest.TestCase):
    def test(self):
        t = FutuBatchOperatorCnDebug(
            from_key="symbol_download_merge/merge.csv",
            key="test_oss"
        )
        self.assertIsNotNone(t.execute(""))


if __name__ == '__main__':
    unittest.main()

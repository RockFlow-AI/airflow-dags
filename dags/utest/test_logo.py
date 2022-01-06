import unittest

from rockflow.dags_test.logo import public_logo_download_debug


class Test(unittest.TestCase):
    def test_market(self):
        self.assertIsNone(public_logo_download_debug.execute(""))


if __name__ == '__main__':
    unittest.main()

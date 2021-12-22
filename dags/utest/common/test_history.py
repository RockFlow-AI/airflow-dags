import unittest

from rockflow.dags.history import *


class Test(unittest.TestCase):
    def test_market(self):
        self.assertIsNotNone(hkex_sync_debug.execute(""))
        self.assertIsNone(us_sync_debug.execute(""))


if __name__ == '__main__':
    unittest.main()

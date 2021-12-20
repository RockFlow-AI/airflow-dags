import unittest

from rockflow.dags.market import *


class Test(unittest.TestCase):
    def test_market(self):
        self.assertIsNotNone(mic_download.execute(""))
        self.assertIsNone(mic_to_mysql.execute(""))


if __name__ == '__main__':
    unittest.main()
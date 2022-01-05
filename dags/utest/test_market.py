import unittest

from rockflow.dags.market import *


class Test(unittest.TestCase):
    def test_mic_download(self):
        self.assertIsNotNone(mic_download.execute(""))

    def test_mic_to_mysql(self):
        self.assertIsNone(mic_to_mysql.execute(""))


if __name__ == '__main__':
    unittest.main()

import unittest

from rockflow.dags_test.logo import logo_import


class Test(unittest.TestCase):
    # def test_market(self):
    #     self.assertIsNone(public_logo_download_debug.execute(""))

    def test_logo_import(self):
        self.assertIsNone(logo_import.execute(""))


if __name__ == '__main__':
    unittest.main()

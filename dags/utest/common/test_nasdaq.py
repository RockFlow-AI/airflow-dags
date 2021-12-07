import unittest

from rockflow.common.nasdaq import Nasdaq
from rockflow.common.proxy import default_proxy


class TestRockFinance(unittest.TestCase):
    def test_nasdaq(self):
        # python -m unittest test_rock_finance.TestRockFinance.test_nasdaq
        nasdaq = Nasdaq(proxy=default_proxy())
        print(nasdaq.get().content)


if __name__ == '__main__':
    unittest.main()

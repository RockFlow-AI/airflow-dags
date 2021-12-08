import unittest

from rockflow.common.nasdaq import Nasdaq
from rockflow.common.proxy import local_proxy


class TestNasdaq(unittest.TestCase):
    def test_nasdaq(self):
        # python -m unittest test_nasdaq.TestNasdaq.test_nasdaq
        nasdaq = Nasdaq(proxy=local_proxy())
        print(nasdaq.get().content)


if __name__ == '__main__':
    unittest.main()

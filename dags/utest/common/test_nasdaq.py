import unittest

from rockflow.common.nasdaq import Nasdaq
from rockflow.common.proxy import local_proxy


class Test(unittest.TestCase):
    def test(self):
        # python -m unittest test_nasdaq.Test.test
        nasdaq = Nasdaq(proxy=local_proxy())
        print(nasdaq.get().content)


if __name__ == '__main__':
    unittest.main()

import unittest

from rockflow.dags.symbol import sink_futu_profile_op


class Test(unittest.TestCase):
    def test_market(self):
        self.assertIsNone(sink_futu_profile_op.execute(""))


if __name__ == '__main__':
    unittest.main()

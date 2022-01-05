import unittest

from rockflow.dags.ice import daily_history


class Test(unittest.TestCase):
    def test_market(self):
        self.assertIsNone(daily_history.execute(""))


if __name__ == '__main__':
    unittest.main()

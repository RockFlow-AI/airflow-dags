import unittest

from rockflow.common.map_helper import join_list


class Test(unittest.TestCase):
    def test(self):
        result = join_list(
            [
                {
                    "symbol": "aapl",
                    "name_zh": "苹果公司",
                    "short_name_a": None,
                    "short_name_b": "苹果B",
                },
            ],
            [
                {
                    "symbol": "aapl",
                    "name_en": "apple",
                    "short_name_a": "苹果A",
                    "short_name_b": None,
                },
            ]
        )
        self.assertIn("aapl", result)
        self.assertIn("symbol", result["aapl"])
        self.assertIn("name_zh", result["aapl"])
        self.assertIn("name_en", result["aapl"])
        self.assertIn("short_name_a", result["aapl"])
        self.assertIn("short_name_b", result["aapl"])
        self.assertNotIn("name_hk", result["aapl"])
        self.assertEqual("apple", result["aapl"]["name_en"])
        self.assertEqual("苹果公司", result["aapl"]["name_zh"])
        self.assertEqual("苹果A", result["aapl"]["short_name_a"])
        self.assertEqual("苹果B", result["aapl"]["short_name_b"])


if __name__ == '__main__':
    unittest.main()

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
        assert "aapl" in result
        assert "symbol" in result["aapl"]
        assert "name_zh" in result["aapl"]
        assert "name_en" in result["aapl"]
        assert "short_name_a" in result["aapl"]
        assert "short_name_b" in result["aapl"]
        assert not "name_hk" in result["aapl"]
        assert "apple" == result["aapl"]["name_en"]
        assert "苹果公司" == result["aapl"]["name_zh"]
        assert "苹果A" == result["aapl"]["short_name_a"]
        assert "苹果B" == result["aapl"]["short_name_b"]


if __name__ == '__main__':
    unittest.main()

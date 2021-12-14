import unittest

from rockflow.common.map_helper import join_map


class Test(unittest.TestCase):
    def test(self):
        result = join_map(
            [
                {
                    "symbol": "aapl",
                    "name_zh": "苹果公司",
                },
            ],
            [
                {
                    "symbol": "aapl",
                    "name_en": "apple",
                },
            ]
        )
        assert "aapl" in result
        assert "symbol" in result["aapl"]
        assert "name_zh" in result["aapl"]
        assert "name_en" in result["aapl"]
        assert not "name_hk" in result["aapl"]
        assert "apple" == result["aapl"]["name_en"]
        assert "苹果公司" == result["aapl"]["name_zh"]


if __name__ == '__main__':
    unittest.main()

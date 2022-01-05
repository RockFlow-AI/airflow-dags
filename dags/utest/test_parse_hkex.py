import unittest

from rockflow.operators.symbol import HkexSymbolParser


class TestHKEX(unittest.TestCase):
    def test_hkex(self):
        hkex = HkexSymbolParser(
            from_key="symbol_download_hkex/h_k_e_x.xlsx",
            key="symbol_download_parse/"
        )
        self.assertIsNotNone(hkex.execute(""))


if __name__ == '__main__':
    unittest.main()

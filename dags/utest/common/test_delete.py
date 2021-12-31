import unittest

from rockflow.common.rename import symbol_match, symbol_rename
from rockflow.operators.rename import RenameOss, DeleteInvalidOss


class TestRename(unittest.TestCase):
    def test_rename(self):
        tests = {
            "700.HK.html": "00700.HK.html",
            "0700.HK.html": "00700.HK.html",
            "1024.HK.html": "01024.HK.html",
            "01024.HK.html": "01024.HK.html",
            "AAPL.US.html": "AAPL.US.html",
            "00700.HK.html": "00700.HK.html",
            "000700.HK.html": "00700.HK.html",
            "000700.SS.html": "00700.SH.html",
            "prefix/700.HK.html": "prefix/00700.HK.html",
            "prefix/0700.HK.html": "prefix/00700.HK.html",
            "prefix/1024.HK.html": "prefix/01024.HK.html",
            "prefix/01024.HK.html": "prefix/01024.HK.html",
            "prefix/AAPL.US.html": "prefix/AAPL.US.html",
            "prefix/00700.HK.html": "prefix/00700.HK.html",
            "prefix/000700.HK.html": "prefix/00700.HK.html",
            "prefix/000700.SS.html": "prefix/00700.SH.html",
        }
        for input, result in tests.items():
            self.assertEqual(symbol_rename(input), result, input)


class TestMatch(unittest.TestCase):
    def test_delete(self):
        tests = {
            "700.HK.html": True,
            "0700.HK.html": True,
            "1024.HK.html": True,
            "01024.HK.html": False,
            "AAPL.US.html": False,
            "00700.HK.html": False,
            "000700.HK.html": True,
            "00700.SS.html": True,
            "00700.SH.html": False,
        }
        for input, result in tests.items():
            self.assertEqual(symbol_match(input), result, input)


class TestDelete(unittest.TestCase):
    def rename(self, key):
        self.assertIsNone(
            RenameOss(
                prefix=key
            ).execute("")
        )

    def delete(self, key):
        self.assertIsNone(
            DeleteInvalidOss(
                prefix=key
            ).execute("")
        )

    def test_rename_cn(self):
        self.rename("symbol_download_futu_company_profile_cn")

    def test_rename_en(self):
        self.rename("symbol_download_futu_company_profile_en")

    # def test_delete_cn(self):
    #     self.delete("company_profile_batch_download_debug_futu_company_profile_cn")
    #
    # def test_delete_en(self):
    #     self.delete("company_profile_batch_download_debug_futu_company_profile_en")


if __name__ == '__main__':
    unittest.main()

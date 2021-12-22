import unittest
from pathlib import Path

from rockflow.dags.const import *
from rockflow.operators.oss import OSSDeleteOperator, OSSRenameOperator


def symbol_match(key):
    return key != symbol_rename(key)


def new_symbol(symbol):
    if symbol.isnumeric():
        return "%05d" % int(symbol)
    else:
        return symbol


def symbol_rename(key):
    name = Path(key).stem
    i = name.rfind('.')
    if 0 < i < len(name) - 1:
        symbol = name[:i]
    else:
        symbol = name
    return key.replace(symbol, new_symbol(symbol))


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
            "prefix/700.HK.html": "prefix/00700.HK.html",
            "prefix/0700.HK.html": "prefix/00700.HK.html",
            "prefix/1024.HK.html": "prefix/01024.HK.html",
            "prefix/01024.HK.html": "prefix/01024.HK.html",
            "prefix/AAPL.US.html": "prefix/AAPL.US.html",
            "prefix/00700.HK.html": "prefix/00700.HK.html",
            "prefix/000700.HK.html": "prefix/00700.HK.html",
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
        }
        for input, result in tests.items():
            self.assertEqual(symbol_match(input), result, input)


class DeleteInvalidOss(OSSDeleteOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def to_delete(self, obj):
        return symbol_match(obj.key)


class RenameOss(OSSRenameOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def dest_name(self, obj):
        return symbol_rename(obj.key)


class TestDelete(unittest.TestCase):
    def rename(self, key):
        self.assertIsNone(
            RenameOss(
                prefix=key,
                region=DEFAULT_REGION,
                bucket_name=DEFAULT_BUCKET_NAME,
                proxy=DEFAULT_PROXY
            ).execute("")
        )

    def delete(self, key):
        self.assertIsNone(
            DeleteInvalidOss(
                prefix=key,
                region=DEFAULT_REGION,
                bucket_name=DEFAULT_BUCKET_NAME,
                proxy=DEFAULT_PROXY
            ).execute("")
        )

    def test_rename_cn(self):
        self.rename("company_profile_batch_download_debug_futu_company_profile_cn")

    def test_rename_en(self):
        self.rename("company_profile_batch_download_debug_futu_company_profile_en")

    def test_delete_cn(self):
        self.delete("company_profile_batch_download_debug_futu_company_profile_cn")

    def test_delete_en(self):
        self.delete("company_profile_batch_download_debug_futu_company_profile_en")


if __name__ == '__main__':
    unittest.main()

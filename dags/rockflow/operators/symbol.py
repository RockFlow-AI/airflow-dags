import os

import pandas as pd

from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq
from rockflow.common.pandas_helper import DataFrameMerger
from rockflow.common.sse import SSE1
from rockflow.common.szse import SZSE1
from rockflow.operators.downloader import DownloadOperator
from rockflow.operators.oss import OSSSaveOperator


class NasdaqSymbolDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return Nasdaq


class HkexSymbolDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return HKEX


class SseSymbolDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return SSE1


class SzseSymbolDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return SZSE1


class SymbolToCsv(OSSSaveOperator):
    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def instance(self):
        return self.exchange(
            proxy=self.proxy
        )

    @property
    def exchange(self):
        raise NotImplementedError()

    @property
    def content(self):
        return self.instance.to_df(
            self.get_object(self.from_key).read()
        ).to_csv()


class NasdaqSymbolToCsv(SymbolToCsv):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return Nasdaq


class HkexSymbolToCsv(SymbolToCsv):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return HKEX


class SseSymbolToCsv(SymbolToCsv):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SSE1


class SzseSymbolToCsv(SymbolToCsv):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SZSE1


class SymbolParser(OSSSaveOperator):
    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def instance(self):
        return self.exchange(
            proxy=self.proxy
        )

    @property
    def exchange(self):
        raise NotImplementedError()

    @property
    def exchange_name(self):
        return self.instance.__class__.__name__.lower()

    @property
    def key(self):
        return os.path.join(self._key, f"{self.exchange_name}.csv")

    def read_csv(self):
        return pd.read_csv(self.get_object(self.from_key))

    @property
    def content(self):
        return self.instance.to_tickers(self.read_csv()).to_csv()


class NasdaqSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return Nasdaq


class HkexSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return HKEX


class SseSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SSE1


class SzseSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SZSE1


class MergeCsvList(OSSSaveOperator):
    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    def get_data_frames(self):
        return [
            pd.read_csv(self.get_object(obj.key))
            for obj in self.object_iterator(self.from_key) if not obj.is_prefix()
        ]

    @property
    def content(self):
        return DataFrameMerger().merge(self.get_data_frames()).to_csv()

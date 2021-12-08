import os

import pandas as pd

from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq
from rockflow.common.pandas_helper import DataFrameMerger
from rockflow.common.sse import SSE1
from rockflow.common.szse import SZSE1
from rockflow.operators.oss import OSSSaveOperator


class SymbolDownloadOperator(OSSSaveOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        raise NotImplementedError()

    @property
    def content(self):
        return self.exchange.get().content


class NasdaqSymbolDownloadOperator(SymbolDownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return Nasdaq(proxy=self.proxy)


class HkexSymbolDownloadOperator(SymbolDownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return HKEX(proxy=self.proxy)


class SseSymbolDownloadOperator(SymbolDownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SSE1(proxy=self.proxy)


class SzseSymbolDownloadOperator(SymbolDownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SZSE1(proxy=self.proxy)


class SymbolToCsv(OSSSaveOperator):
    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def exchange(self):
        raise NotImplementedError()

    @property
    def content(self):
        return self.exchange.to_df(
            self.get_object(self.from_key).read()
        ).to_csv()


class NasdaqSymbolToCsv(SymbolToCsv):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return Nasdaq()


class HkexSymbolToCsv(SymbolToCsv):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return HKEX()


class SseSymbolToCsv(SymbolToCsv):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SSE1()


class SzseSymbolToCsv(SymbolToCsv):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SZSE1()


class SymbolParser(OSSSaveOperator):
    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def exchange(self):
        raise NotImplementedError()

    @property
    def key(self):
        return os.path.join(self._key, f"{self.exchange.__name__}.csv")

    @property
    def content(self):
        return self.exchange.to_tickers(
            pd.read_csv(self.get_object(self.from_key))
        ).to_csv()


class NasdaqSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return Nasdaq()


class HkexSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return HKEX()


class SseSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SSE1()


class SzseSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SZSE1()


class MergeCsvList(OSSSaveOperator):
    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    def get_data_frames(self):
        return [pd.read_csv(oss.read()) for oss in self.object_iterator(self.from_key)]

    @property
    def content(self):
        return DataFrameMerger().merge(self.get_data_frames()).to_csv()

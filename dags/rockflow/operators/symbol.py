import os

import pandas as pd

from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq
from rockflow.common.pandas_helper import merge_data_frame
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


class SymbolParser(OSSSaveOperator):
    template_fields = ["from_key"]

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
    def oss_key(self):
        return os.path.join(self.key, f"{self.instance.lowercase_class_name}.csv")

    def read_raw(self):
        return self.instance.to_df(self.get_object(self.from_key).read())

    @property
    def content(self):
        return self.instance.to_tickers(self.read_raw()).to_csv()


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

    @property
    def oss_key(self):
        return self.key

    def get_data_frames(self):
        return [
            pd.read_csv(self.get_object(obj.key))
            for obj in self.object_iterator(self.from_key) if not obj.is_prefix()
        ]

    @property
    def content(self):
        return merge_data_frame(self.get_data_frames()).to_csv()

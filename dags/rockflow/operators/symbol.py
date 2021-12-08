import pandas as pd

from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq
from rockflow.common.pandas_helper import DataFrameMerger
from rockflow.common.sse import SSE1
from rockflow.common.szse import SZSE1
from rockflow.operators.oss import OSSOperator


class SymbolDownloadOperator(OSSOperator):
    def __init__(
            self,
            key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key

    @property
    def exchange(self):
        raise NotImplementedError()

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.key,
            content=Nasdaq(proxy=self.proxy).get().content
        )


class NasdaqSymbolDownloadOperator(SymbolDownloadOperator):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return Nasdaq(proxy=self.proxy)


class HkexSymbolDownloadOperator(SymbolDownloadOperator):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return HKEX(proxy=self.proxy)


class SseSymbolDownloadOperator(SymbolDownloadOperator):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SSE1(proxy=self.proxy)


class SzseSymbolDownloadOperator(SymbolDownloadOperator):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SZSE1(proxy=self.proxy)


class SymbolToCsv(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    @property
    def exchange(self):
        raise NotImplementedError()

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=self.exchange.to_df(
                self.get_object(self.from_key).read()
            ).to_csv()
        )


class NasdaqSymbolToCsv(SymbolToCsv):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return Nasdaq()


class HkexSymbolToCsv(OSSOperator):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return HKEX()


class SseSymbolToCsv(SymbolToCsv):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SSE1()


class SzseSymbolToCsv(SymbolToCsv):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SZSE1()


class SymbolParser(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    @property
    def exchange(self):
        raise NotImplementedError()

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=self.exchange.to_tickers(
                pd.read_csv(self.get_object(self.from_key))
            ).to_csv()
        )


class NasdaqSymbolParser(SymbolParser):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return Nasdaq()


class HkexSymbolParser(SymbolParser):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return HKEX()


class SseSymbolParser(SymbolParser):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SSE1()


class SzseSymbolParser(SymbolParser):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return SZSE1()


class MergeCsvList(OSSOperator):
    def __init__(
            self,
            from_key_list: list,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key_list = from_key_list
        self.to_key = to_key

    def get_data_frames(self):
        result = []
        for from_key in self.from_key_list:
            result.append(pd.read_csv(self.get_object(from_key)))
        return result

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=DataFrameMerger().merge(self.get_data_frames()).to_csv()
        )

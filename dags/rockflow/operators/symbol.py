import pandas as pd

from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq
from rockflow.common.sse import SSE1
from rockflow.common.szse import SZSE1
from rockflow.common.pandas_helper import DataFrameMerger
from rockflow.common.sse import SSE1
from rockflow.operators.oss import OSSOperator


class NasdaqSymbolDownloadOperator(OSSOperator):
    def __init__(
            self,
            key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.key,
            content=Nasdaq(proxy=self.proxy).get().content
        )


class NasdaqSymbolToCsv(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=Nasdaq().to_df(
                self.get_object(self.from_key).read()
            ).to_csv()
        )


class HkexSymbolDownloadOperator(OSSOperator):
    def __init__(
            self,
            key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.key,
            content=HKEX(proxy=self.proxy).get().content
        )


class HkexSymbolToCsv(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=HKEX().to_df(
                self.get_object(self.from_key).read()
            ).to_csv()
        )


class NasdaqSymbolParser(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=Nasdaq().to_tickers(
                pd.read_csv(self.get_object(self.from_key))
            ).to_csv()
        )


class HkexSymbolParser(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=HKEX().to_tickers(
                pd.read_csv(self.get_object(self.from_key))
            ).to_csv()
        )

class SseSymbolDownloadOperator(OSSOperator):
    def __init__(
            self,
            key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.key,
            content=SSE1(proxy=self.proxy).get().content
        )


class SseSymbolToCsv(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=SSE1().to_df(
                self.get_object(self.from_key).read()
            ).to_csv()
        )

class SseSymbolParser(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

class SzseSymbolDownloadOperator(OSSOperator):
    def __init__(
            self,
            key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.key,
            content=SZSE1(proxy=self.proxy).get().content
        )


class SzseSymbolToCsv(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=SZSE1().to_df(
                self.get_object(self.from_key).read()
            ).to_csv()
        )

class SzseSymbolParser(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=SZSE1().to_tickers(
                pd.read_csv(self.get_object(self.from_key))
            ).to_csv()
        )

class SseSymbolDownloadOperator(OSSOperator):
    def __init__(
            self,
            key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.key,
            content=SSE1(proxy=self.proxy).get().content
        )


class SseSymbolToCsv(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=SSE1().to_df(
                self.get_object(self.from_key).read()
            ).to_csv()
        )


class SseSymbolParser(OSSOperator):
    def __init__(
            self,
            from_key: str,
            to_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        self.oss_hook.load_string(
            bucket_name=self.bucket_name,
            key=self.to_key,
            content=SSE1().to_tickers(
                pd.read_csv(self.get_object(self.from_key))
            ).to_csv()
        )


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

import os
from multiprocessing.pool import ThreadPool as Pool

import pandas as pd
from airflow import AirflowException

from rockflow.common.apollo_nasdaq import ApolloNasdaq
from rockflow.common.apollo_nyse import ApolloNYSE
from rockflow.common.apollo_hkex import ApolloHKEX
from rockflow.common.apollo_otc import ApolloOTC
from rockflow.common.pandas_helper import merge_data_frame_by_column
from rockflow.common.sse import SSE1
from rockflow.common.szse import SZSE1
from rockflow.operators.const import DEFAULT_POOL_SIZE
from rockflow.operators.downloader import DownloadOperator
from rockflow.operators.oss import OSSSaveOperator


class NasdaqSymbolDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return ApolloNasdaq


class NyseSymbolDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return ApolloNYSE


class OtcSymbolDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return ApolloOTC


class HkexSymbolDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return ApolloHKEX


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
        return self.instance.to_tickers(self.read_raw()).to_csv(index=False)


class NasdaqSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return ApolloNasdaq


class NyseSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return ApolloNYSE


class OtcSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return ApolloOTC


class HkexSymbolParser(SymbolParser):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def exchange(self):
        return ApolloHKEX


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
            pool_size: int = DEFAULT_POOL_SIZE,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.pool_size = pool_size

    @property
    def oss_key(self):
        return self.key

    def read_one(self, obj):
        return pd.read_csv(self.get_object(obj.key), index_col=False)

    def get_data_frames(self):
        with Pool(self.pool_size) as pool:
            return pool.map(
                lambda x: self.read_one(
                    x), self.path_object_iterator(self.from_key)
            )

    @property
    def content(self):
        result = merge_data_frame_by_column(self.get_data_frames())
        if result.isna().any().any():
            raise AirflowException(f"Has Nan: {result}")
        return result.to_csv(index=False)

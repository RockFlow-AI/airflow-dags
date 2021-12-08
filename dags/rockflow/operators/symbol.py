import pandas as pd

from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq
from rockflow.common.pandas_helper import DataFrameMerger
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
        r = Nasdaq(proxy=self.proxy).get()
        self.oss_hook.load_string(bucket_name=self.bucket_name, key=self.key, content=r.content)


class NasdaqSymbolToCSV(OSSOperator):
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
        raw_df = Nasdaq().to_df(self.get_object(self.from_key))
        self.oss_hook.load_string(bucket_name=self.bucket_name, key=self.to_key, content=raw_df.to_csv())


class HkexSymbolDownloadOperator(OSSOperator):
    def __init__(
            self,
            key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key

    def execute(self, context):
        r = HKEX(proxy=self.proxy).get()
        self.oss_hook.load_string(bucket_name=self.bucket_name, key=self.key, content=r.content)


class HkexSymbolToCSV(OSSOperator):
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
        raw_df = HKEX().to_df(self.get_object(self.from_key).read())
        self.oss_hook.load_string(bucket_name=self.bucket_name, key=self.to_key, content=raw_df.to_csv())


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
        merged_df = DataFrameMerger().merge(self.get_data_frames())
        self.oss_hook.load_string(bucket_name=self.bucket_name, key=self.to_key, content=merged_df.to_csv())

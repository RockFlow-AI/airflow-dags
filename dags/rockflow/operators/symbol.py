from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq
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
        self.oss_hook.load_string(self.to_key, raw_df.to_csv())


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
        raw_df = HKEX().to_df(self.get_object(self.from_key))
        self.oss_hook.load_string(self.to_key, raw_df.to_csv())


class MergeSymbolList(OSSOperator):
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
        pass
        # call tickers
        # save to oss
        # raw_df = HKEX().to_df(self.get_object(self.from_key))
        # self.oss_hook.load_string(self.to_key, raw_df.to_csv())

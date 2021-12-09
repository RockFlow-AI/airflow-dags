import os
from typing import Any

import pandas as pd
from stringcase import snakecase

from rockflow.common.futu_company_profile import FutuCompanyProfileCn, FutuCompanyProfileEn
from rockflow.operators.oss import OSSSaveOperator, OSSOperator


class FutuBatchOperator(OSSOperator):
    def __init__(self,
                 from_key: str,
                 key: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.key = key

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))

    @staticmethod
    def call_one(cls, line: pd.Series, prefix: str, proxy, bucket):
        obj = cls(
            symbol=line['yahoo'],
            futu_ticker=line['futu'],
            prefix=prefix,
            proxy=proxy
        )
        if not FutuBatchOperator.object_exists(bucket, obj.oss_key):
            FutuBatchOperator.put_object(bucket, obj.oss_key, obj.get().content)

    @staticmethod
    def call(line: pd.Series, prefix, proxy, bucket):
        cls_list = [
            FutuCompanyProfileCn,
            FutuCompanyProfileEn,
        ]
        [FutuBatchOperator.call_one(cls, line, prefix, proxy, bucket) for cls in cls_list]

    def execute(self, context: Any):
        print(f"symbol: {self.symbols[:10]}")
        self.symbols.parallel_apply(
            FutuBatchOperator.call,
            axis=1,
            args=(self.key, self.proxy, self.bucket)
        )


class FutuOperator(OSSSaveOperator):
    def __init__(self,
                 ticker: str,
                 **kwargs) -> None:
        if 'task_id' not in kwargs:
            kwargs['task_id'] = f"{snakecase(self.__class__.__name__)}_{ticker}"
        super().__init__(**kwargs)
        self.ticker = ticker

    @property
    def key(self):
        return os.path.join(self._key, f"{self.ticker}.html")

    @property
    def page(self):
        raise NotImplementedError()

    @property
    def instance(self):
        return self.page(
            symbol=self.ticker,
            futu_ticker=self.ticker,
            proxy=self.proxy,
        )

    @property
    def content(self):
        return self.instance.get().content


class FutuCnOperator(FutuOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def page(self):
        return FutuCompanyProfileCn


class FutuEnOperator(FutuOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def page(self):
        return FutuCompanyProfileEn

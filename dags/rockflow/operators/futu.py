import os
from typing import Any

import pandas as pd
from stringcase import snakecase

from rockflow.common.futu_company_profile import FutuCompanyProfileCn, FutuCompanyProfileEn
from rockflow.operators.oss import OSSSaveOperator, OSSOperator


def parallel_func(line: pd.Series, prefix, proxy, bucket):
    symbol = line['symbol']
    futu_ticker = line['futu']
    cn = FutuCompanyProfileCn(
        symbol=symbol,
        futu_ticker=futu_ticker,
        prefix=prefix,
        proxy=proxy
    )
    bucket.put_object(cn.oss_key, cn.get().content)
    en = FutuCompanyProfileEn(
        symbol=symbol,
        futu_ticker=futu_ticker,
        prefix=prefix,
        proxy=proxy
    )
    bucket.put_object(en.oss_key, en.get().content)


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
        return pd.read_csv(self.get_object(self.from_key))[:10]

    def execute(self, context: Any):
        print(f"symbol: {self.symbols}")
        self.symbols.apply(
            parallel_func,
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

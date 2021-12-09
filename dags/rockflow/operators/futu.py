import json
import os
from pathlib import Path
from typing import Any

import oss2
import pandas as pd
from stringcase import snakecase

from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.futu_company_profile import FutuCompanyProfileCn, FutuCompanyProfileEn, FutuCompanyProfile
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
    def object_not_update_for_a_week(bucket: oss2.api.Bucket, key: str):
        if not FutuBatchOperator.object_exists_(bucket, key):
            return False
        return GmtDatetimeCheck(
            FutuBatchOperator.last_modified_(bucket, key), weeks=1
        )

    @staticmethod
    def call_one(cls, line: pd.Series, prefix: str, proxy, bucket):
        obj = cls(
            symbol=line['yahoo'],
            futu_ticker=line['futu'],
            prefix=prefix,
            proxy=proxy
        )
        if not FutuBatchOperator.object_not_update_for_a_week(bucket, obj.oss_key):
            FutuBatchOperator.put_object_(bucket, obj.oss_key, obj.get().content)

    @staticmethod
    def call(line: pd.Series, prefix, proxy, bucket):
        cls_list = [
            FutuCompanyProfileCn,
            FutuCompanyProfileEn,
        ]
        [FutuBatchOperator.call_one(cls, line, prefix, proxy, bucket) for cls in cls_list]

    def execute(self, context: Any):
        print(f"symbol: {self.symbols[:10]}")
        self.symbols.apply(
            FutuBatchOperator.call,
            axis=1,
            args=(self.key, self.proxy, self.bucket)
        )


class FutuExtractHtml(OSSSaveOperator):
    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    def symbol(self, obj):
        return Path(obj.key).stem

    def extract_data(self, obj):
        return FutuCompanyProfile.extract_data(
            self.get_object(obj.key), self.symbol(obj)
        )

    @property
    def content(self):
        result = {}
        for obj in self.object_iterator(self.from_key):
            if obj.is_prefix():
                result.update({
                    self.symbol(sub_obj): self.extract_data(sub_obj)
                    for sub_obj in self.object_iterator(obj.key) if not sub_obj.is_prefix()
                })
            else:
                result[self.symbol(obj)] = self.extract_data(obj)
        return json.dumps(result, ensure_ascii=False)


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

import json
import os
from itertools import islice
from multiprocessing.pool import ThreadPool as Pool
from pathlib import Path
from typing import Any

import oss2
import pandas as pd

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


class FutuBatchOperatorDebug(FutuBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))[:100]


class FutuExtractHtml(OSSSaveOperator):
    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def key(self):
        return os.path.join(self._key, f"{self._key}.json")

    @staticmethod
    def symbol(obj):
        return Path(obj.key).stem

    @staticmethod
    def extract_data(bucket, obj):
        return FutuCompanyProfile.extract_data(
            FutuExtractHtml.get_object_(bucket, obj.key), FutuExtractHtml.symbol(obj)
        )

    @staticmethod
    def task(bucket, obj):
        if obj.is_prefix():
            return
        return FutuExtractHtml.extract_data(bucket, obj)

    @property
    def content(self):
        result = []
        for obj in self.object_iterator(self.from_key):
            if not obj.is_prefix():
                continue
            with Pool(processes=24) as pool:
                result.append(
                    pool.map(
                        lambda x: FutuExtractHtml.task(self.bucket, x), self.object_iterator_(self.bucket, obj.key)
                    )
                )
        return json.dumps(result, ensure_ascii=False)


class FutuExtractHtmlDebug(FutuExtractHtml):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def content(self):
        result = []
        for obj in self.object_iterator(self.from_key):
            if not obj.is_prefix():
                continue
            with Pool(processes=24) as pool:
                result.append(
                    pool.map(
                        lambda x: FutuExtractHtml.task(self.bucket, x),
                        islice(self.object_iterator_(self.bucket, obj.key), 24)
                    )
                )
        return json.dumps(result, ensure_ascii=False)

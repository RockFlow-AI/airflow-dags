import json
import os
from io import BytesIO
from multiprocessing.pool import ThreadPool as Pool
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
    def call(line: pd.Series, cls, prefix: str, proxy, bucket):
        obj = cls(
            symbol=line['yahoo'],
            futu_ticker=line['futu'],
            prefix=prefix,
            proxy=proxy
        )
        if not FutuBatchOperator.object_not_update_for_a_week(bucket, obj.oss_key):
            r = obj.get()
            if not r:
                return
            FutuBatchOperator.put_object_(bucket, obj.oss_key, r.content)

    @property
    def cls(self):
        raise NotImplementedError()

    @property
    def oss_key(self):
        return f"{self.key}_{snakecase(self.cls.__name__)}"

    def execute(self, context: Any):
        print(f"symbol: {self.symbols[:10]}")
        self.symbols.apply(
            FutuBatchOperator.call,
            axis=1,
            args=(self.cls, self.key, self.proxy, self.bucket)
        )
        return self.oss_key


class FutuBatchOperatorCn(FutuBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return FutuCompanyProfileCn


class FutuBatchOperatorEn(FutuBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return FutuCompanyProfileEn


class FutuBatchOperatorCnDebug(FutuBatchOperatorCn):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))[:100]


class FutuBatchOperatorEnDebug(FutuBatchOperatorEn):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))[:100]


class FutuExtractHtml(OSSSaveOperator):
    template_fields = ["from_key"]

    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def oss_key(self):
        return os.path.join(
            f"{self.key}_{self.snakecase_class_name}_{self.from_key}",
            f"{self.snakecase_class_name}.json"
        )

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
        with Pool(processes=24) as pool:
            result = pool.map(
                lambda x: FutuExtractHtml.task(self.bucket, x), self.object_iterator_(self.bucket, f"{self.from_key}/")
            )
            return json.dumps(result, ensure_ascii=False)


class FutuFormatJson(OSSSaveOperator):
    template_fields = ["from_key"]

    def __init__(
            self,
            from_key: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def oss_key(self):
        return os.path.join(
            f"{self.key}_{self.snakecase_class_name}_{snakecase(self.cls.__name__)}",
            f"{self.snakecase_class_name}.json"
        )

    @property
    def cls(self):
        raise NotImplementedError()

    @property
    def content(self):
        result = [
            self.cls.format_(self.cls.language(), i)
            for i in json.load(
                BytesIO(self.get_object_(self.bucket, self.from_key).read())
            )
        ]
        return json.dumps(result, ensure_ascii=False)


class FutuFormatJsonCn(FutuFormatJson):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return FutuCompanyProfileCn


class FutuFormatJsonEn(FutuFormatJson):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return FutuCompanyProfileEn

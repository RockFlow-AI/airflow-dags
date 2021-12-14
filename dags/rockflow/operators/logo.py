from typing import Any

import oss2
import pandas as pd

from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.logo import Public, Etoro
from rockflow.operators.oss import OSSOperator


class LogoBatchOperator(OSSOperator):
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
        # TODO(speed up)
        if LogoBatchOperator.object_exists_(bucket, key):
            return True
        if not LogoBatchOperator.object_exists_(bucket, key):
            return False
        return GmtDatetimeCheck(
            LogoBatchOperator.last_modified_(bucket, key), weeks=1
        )

    @staticmethod
    def call(line: pd.Series, cls, prefix, proxy, bucket):
        obj = cls(
            symbol=line['yahoo'],
            prefix=prefix,
            proxy=proxy
        )
        if not LogoBatchOperator.object_not_update_for_a_week(bucket, obj.oss_key):
            r = obj.get()
            if not r:
                return
            LogoBatchOperator.put_object_(bucket, obj.oss_key, r.content)

    @property
    def cls(self):
        raise NotImplementedError()

    def execute(self, context: Any):
        print(f"symbol: {self.symbols[:10]}")
        self.symbols.apply(
            LogoBatchOperator.call,
            axis=1,
            args=(self.cls, self.key, self.proxy, self.bucket)
        )


class PublicLogoBatchOperator(LogoBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return Public


class PublicLogoBatchOperatorDebug(PublicLogoBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))[:100]


class EtoroLogoBatchOperator(LogoBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def cls(self):
        return Etoro


class EtoroLogoBatchOperatorDebug(EtoroLogoBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))[:100]

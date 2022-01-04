from typing import Any

import pandas as pd
from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.logo import Public, Etoro
from rockflow.operators.common import is_none_us_symbol
from rockflow.operators.const import GLOBAL_DEBUG
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

    def object_not_update_for_a_week(self, key: str):
        if not self.object_exists(key):
            return True
        elif GLOBAL_DEBUG:
            return False
        try:
            return GmtDatetimeCheck(
                self.last_modified(key), weeks=1
            ).check
        except Exception as e:
            self.log.error(f"error: {str(e)}")
            return True

    def save_one(self, line: pd.Series, cls):
        symbol = line['yahoo']
        if is_none_us_symbol(symbol):
            return
        obj = cls(
            symbol=symbol,
            prefix=self.key,
            proxy=self.proxy
        )
        if self.object_not_update_for_a_week(obj.oss_key):
            r = obj.get()
            if not r:
                return
            self.put_object(obj.oss_key, r.content)

    @property
    def cls(self):
        raise NotImplementedError()

    def execute(self, context: Any):
        self.log.info(f"symbol: {self.symbols}")
        self.symbols.apply(
            self.save_one,
            axis=1,
            args=(self.cls, self.key)
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

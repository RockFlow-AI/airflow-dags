from typing import Any
from io import StringIO

import oss2
import pandas as pd
import json

from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.yahoo import Yahoo
from rockflow.operators.oss import OSSOperator, OSSSaveOperator


class YahooBatchOperator(OSSOperator):
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
        if YahooBatchOperator.object_exists_(bucket, key):
            return True
        if not YahooBatchOperator.object_exists_(bucket, key):
            return False
        return GmtDatetimeCheck(
            YahooBatchOperator.last_modified_(bucket, key), days=1
        )

    @staticmethod
    def call(line: pd.Series, prefix, proxy, bucket):
        obj = Yahoo(
            symbol=line['yahoo'],
            prefix=prefix,
            proxy=proxy
        )
        if not YahooBatchOperator.object_not_update_for_a_week(bucket, obj.oss_key):
            r = obj.get()
            if not r:
                return
            YahooBatchOperator.put_object_(bucket, obj.oss_key, r.content)

    def execute(self, context: Any):
        print(f"symbol: {self.symbols[:10]}")
        self.symbols.apply(
            YahooBatchOperator.call,
            axis=1,
            args=(self.key, self.proxy, self.bucket)
        )


class YahooBatchOperatorDebug(YahooBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))[:100]


class YahooExtractAssetProfileOperator(OSSSaveOperator):
    def __init__(self,
                 from_key: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def oss_key(self):
        return self.key+'/'+self.key+'.json'

    def get_data(self):
        json_data_list = [
            [
                obj.key,
                json.loads(
                    self.get_object(obj.key).read()
                ).get("quoteSummary").get("result")[0]
            ]
            for obj in self.object_iterator(self.from_key+'/') if not obj.is_prefix()
        ]
        return json_data_list

    def merge_data(self):
        result = {}
        for key, item in self.get_data():
            symbol = key.split('/')[-1].replace('.json', '')
            result[symbol] = item["assetProfile"]
        return result

    @property
    def content(self):
        return json.dumps(self.merge_data())

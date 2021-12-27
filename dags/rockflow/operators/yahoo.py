import json
import logging
import os
from multiprocessing.pool import ThreadPool as Pool
from pathlib import Path
from typing import Any

import oss2
import pandas as pd
from stringcase import snakecase

from rockflow.common.const import DEFAULT_POOL_SIZE
from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.pandas_helper import merge_data_frame_by_index
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
            symbol=line['rockflow'],
            yahoo=line['yahoo'],
            prefix=prefix,
            proxy=proxy
        )
        if not YahooBatchOperator.object_not_update_for_a_week(bucket, obj.oss_key):
            r = obj.get()
            if not r:
                return
            YahooBatchOperator.put_object_(bucket, obj.oss_key, r.content)

    def execute(self, context: Any):
        self.log.info(f"symbol: {self.symbols[:10]}")
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


class YahooExtractOperator(OSSSaveOperator):
    template_fields = ["from_key"]

    def __init__(self,
                 from_key: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key

    @property
    def oss_key(self):
        return self.key

    def read_data_pandas(self, obj):
        symbol = self._get_filename(obj.key)
        if obj.is_prefix():
            return pd.DataFrame.from_dict({symbol: None}, orient='index')
        json_dic = json.loads(
            self.get_object(obj.key).read())
        try:
            json_data = json_dic.get("quoteSummary").get("result")[0]
            return pd.DataFrame.from_dict(
                {symbol: json_data
                 }, orient='index')
        except:
            logging.error(
                f"Error occurred while reading json! File: {obj.key} skipped.")
            return pd.DataFrame.from_dict({symbol: None}, orient='index')

    def _get_data(self):
        with Pool(DEFAULT_POOL_SIZE) as pool:
            result = pool.map(
                lambda x: self.read_data_pandas(x), self.object_iterator(
                    os.path.join(self.from_key, ""))
            )
            return result

    def _get_filename(self, file_path):
        return Path(file_path).stem

    def _save_key(self, key):
        return os.path.join(self.oss_key + '_' + snakecase(key), snakecase(key) + '.json')

    @property
    def content(self):
        data = merge_data_frame_by_index(self._get_data())
        result = []
        for category in data:
            result.append(
                [category,
                 json.dumps(data[category].to_dict())])
        return result

    def execute(self, context):
        with Pool(DEFAULT_POOL_SIZE) as pool:
            pool.map(
                lambda x: self.put_object(self._save_key(x[0]), x[1]),
                self.content
            )
            return self.oss_key

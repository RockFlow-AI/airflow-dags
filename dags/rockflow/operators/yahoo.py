from typing import Any
from io import StringIO
from multiprocessing.pool import ThreadPool as Pool
from pathlib import Path

import os
import oss2
import pandas as pd
import json

from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.yahoo import Yahoo
from rockflow.operators.oss import OSSOperator, OSSSaveOperator
from stringcase import snakecase


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

    def _list_file(self):
        return [obj for obj in self.object_iterator(self.from_key) if not obj.is_prefix()]

    def _get_data(self):
        json_data_list = []
        for obj in self._list_file():
            json_dic = json.loads(self.get_object(obj.key).read())
            try:
                json_data = json_dic.get("quoteSummary").get("result")[0]
            except:
                print("Error occurred while reading json! File:",
                      obj.key, "skipped.")
            else:
                json_data_list.append([self._get_filename(obj.key), json_data])
        return json_data_list

    def _get_filename(self, file_path):
        return Path(file_path).stem

    @staticmethod
    def merge_data(data_list):
        result = {}
        for symbol, item in data_list:
            for key in item:
                if key not in result:
                    result[key] = {}
                if symbol not in result[key]:
                    result[key][symbol] = item[key]
                else:
                    print("Duplicate symbol:", symbol,
                          "while merging key:", key)
        return result

    def _save_key(self, key):
        return os.path.join(self.oss_key + '_' + snakecase(key), snakecase(key) + '.json')

    @property
    def content(self):
        data = self.merge_data(self._get_data())
        result = []
        for category in data:
            result.append([category, json.dumps(data[category])])
        return result

    def execute(self, context):
        with Pool(processes=24) as pool:
            pool.map(
                lambda x: self.put_object(self._save_key(x[0]), x[1]),
                self.content
            )
        return self.oss_key

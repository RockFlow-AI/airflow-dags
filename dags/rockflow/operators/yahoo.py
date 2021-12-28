import json
import logging
import os
from multiprocessing.pool import ThreadPool as Pool
from pathlib import Path
from typing import Any, Dict

import oss2
import pandas as pd
from stringcase import snakecase

from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.pandas_helper import merge_data_frame_by_index
from rockflow.common.yahoo import Yahoo
from rockflow.operators.const import DEFAULT_POOL_SIZE
from rockflow.operators.mysql import OssToMysqlOperator
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
        if symbol.endswith("HK") or symbol.endswith("SZ") or symbol.endswith("SS"):
            return
        if obj.is_prefix():
            return
        json_dic = json.loads(
            self.get_object(obj.key).read()
        )
        try:
            json_data = json_dic.get("quoteSummary").get("result")[0]
            return pd.DataFrame.from_dict(
                {symbol: json_data},
                orient='index'
            )
        except:
            logging.error(
                f"Error occurred while reading json! File: {obj.key} skipped.")
            return

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
            result = [
                category,
                json.dumps(data[category].to_dict())
            ]
            yield result

    def execute(self, context):
        for x in self.content:
            self.put_object(self._save_key(x[0]), x[1])
        return self.oss_key


class SummaryDetailImportOperator(OssToMysqlOperator):
    def __init__(self, **kwargs) -> None:
        if 'index_col' not in kwargs:
            kwargs['index_col'] = "symbol"
        if 'mapping' not in kwargs:
            kwargs['mapping'] = {
                "symbol": "symbol",
                "open": "open",
                "dayHigh": "high",
                "dayLow": "low",
                "previousClose": "previous_close",
                "marketCap": "market_cap",
                "volume": "volume",
                "trailingPE": "trailing_pe",
                "dividendYield": "dividend_yield",
                "currency": "currency",
            }
        super().__init__(**kwargs)

    def format_dict(self, dict_data):
        dict_data = {
            k: v for k, v in dict_data.items() if isinstance(v, Dict)
        }
        for k, v in dict_data.items():
            if not isinstance(v, Dict):
                continue
            v[self.index_col] = k
            for key, value in v.items():
                if not isinstance(value, Dict):
                    continue
                if "raw" in value:
                    # 去除所有带view的展示
                    v[key] = value["raw"]
                elif not value:
                    v[key] = None
        return dict_data

    def extract_data(self) -> pd.DataFrame:
        return self.extract_index_dict_to_df(
            self.format_dict(self.extract_index_dict())
        )

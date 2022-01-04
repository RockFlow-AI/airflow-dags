import json
import logging
import os
from multiprocessing.pool import ThreadPool as Pool
from typing import Any, Dict

import pandas as pd
from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.pandas_helper import merge_data_frame_by_index
from rockflow.common.yahoo import Yahoo
from rockflow.operators.common import is_none_us_symbol, is_us_symbol
from rockflow.operators.const import DEFAULT_POOL_SIZE
from rockflow.operators.mysql import OssToMysqlOperator
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

    def object_not_update_for_a_day(self, key: str) -> bool:
        if not self.object_exists(key):
            return True
        try:
            return GmtDatetimeCheck(
                self.last_modified(key), days=1
            )
        except Exception as e:
            self.log.error(f"error: {str(e)}")
            return True

    def save_one(self, line: pd.Series):
        obj = Yahoo(
            symbol=line['rockflow'],
            yahoo=line['yahoo'],
            prefix=self.prefix,
            proxy=self.proxy
        )
        if self.object_not_update_for_a_day(obj.oss_key):
            r = obj.get()
            if not r:
                return
            self.put_object(obj.oss_key, r.content)

    def execute(self, context: Any):
        self.log.info(f"symbol: {self.symbols}")
        self.symbols.apply(
            self.save_one,
            axis=1,
            args=(self.key,)
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
                 symbol_key: str,
                 pool_size: int = DEFAULT_POOL_SIZE,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.symbol_key = symbol_key

        self.pool_size = pool_size

    @property
    def oss_key(self):
        return self.key

    def split(self, symbol):
        raise NotImplementedError()

    def read_one(self, symbol):
        if not self.split(symbol):
            return
        symbol_key = self.get_symbol_key(symbol)
        try:
            json_dic = json.loads(
                self.get_object(symbol_key).read()
            )
        except:
            logging.error(
                f"Error occurred while downloading file: {symbol_key}")
            return
        try:
            json_data = json_dic.get("quoteSummary").get("result")[0]
            return pd.DataFrame.from_dict(
                {symbol: json_data},
                orient='index'
            )
        except:
            logging.error(
                f"Error occurred while reading json! File: {symbol_key} skipped.")
            return

    @property
    def symbol_list(self):
        symbol_df = pd.read_csv(self.get_object(
            self.symbol_key), index_col=False)
        return symbol_df["rockflow"].to_list()

    def merge_data(self):
        with Pool(self.pool_size) as pool:
            result = pool.map(
                lambda x: self.read_one(x), self.symbol_list
            )
            return result

    def get_symbol_key(self, symbol):
        return os.path.join(self.from_key, symbol + '.json')

    def save_key(self, key):
        return os.path.join(self.key + '_' + snakecase(key), self.snakecase_class_name + '.json')

    @property
    def content(self):
        data = merge_data_frame_by_index(self.merge_data())
        if data is None:
            return
        for category in data:
            result = [
                category,
                json.dumps(data[category].to_dict())
            ]
            yield result

    def execute(self, context):
        for x in self.content:
            self.put_object(self.save_key(x[0]), x[1])


class YahooExtractOperatorUS(YahooExtractOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def split(self, symbol):
        return is_us_symbol(symbol)


class YahooExtractOperatorNoneUS(YahooExtractOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def split(self, symbol):
        return is_none_us_symbol(symbol)


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

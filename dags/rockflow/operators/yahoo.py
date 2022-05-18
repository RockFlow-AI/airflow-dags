import json
import logging
import os
import time
from multiprocessing.pool import ThreadPool as Pool
from typing import Any, Hashable, Dict

import pandas as pd
from stringcase import snakecase

from rockflow.common.datatime_helper import GmtDatetimeCheck
from rockflow.common.pandas_helper import merge_data_frame_by_index
from rockflow.common.yahoo import Yahoo
from rockflow.operators.const import DEFAULT_POOL_SIZE, GLOBAL_DEBUG
from rockflow.operators.mysql import OssToMysqlOperator
from rockflow.operators.oss import OSSOperator, OSSSaveOperator


class YahooBatchOperator(OSSOperator):
    def __init__(self,
                 from_key: str,
                 key: str,
                 pool_size: int = DEFAULT_POOL_SIZE,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.key = key
        self.pool_size = pool_size

    @property
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.from_key))

    def object_not_update_for_a_day(self, key: str) -> bool:
        if not self.object_exists(key):
            return True
        elif GLOBAL_DEBUG:
            return False
        try:
            return GmtDatetimeCheck(
                self.last_modified(key), days=1
            ).check
        except Exception as e:
            self.log.error(f"error: {str(e)}")
            return True

    def save_one(self, line: tuple[Hashable, pd.Series]):
        obj = Yahoo(
            symbol=line[1]['rockflow'],
            yahoo=line[1]['yahoo'],
            prefix=self.key,
            proxy=self.proxy
        )
        if self.object_not_update_for_a_day(obj.oss_key):
            r = obj.get()
            if not r:
                return
            self.put_object(obj.oss_key, r.content)
            time.sleep(0.5)

    def execute(self, context: Any):
        self.log.info(f"symbol: {self.symbols}")
        # # TODO(daijunkai)需要确认并发
        # self.symbols.apply(
        #     self.save_one,
        #     axis=1
        # )
        with Pool(self.pool_size) as pool:
            pool.map(
                lambda x: self.save_one(x), self.symbols.iterrows()
            )


class YahooBatchOperatorDebug(YahooBatchOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def symbols(self) -> pd.DataFrame:
        return super().symbols[:100]


class YahooExtractOperator(OSSSaveOperator):
    template_fields = ["from_key"]

    def __init__(self,
                 from_key: str,
                 symbol_key: str,
                 partition: int,  # 分区编号
                 sharding: int,  # 分片数量
                 pool_size: int = DEFAULT_POOL_SIZE,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.symbol_key = symbol_key
        self.partition = partition
        self.sharding = sharding

        self.pool_size = pool_size

    @property
    def oss_key(self):
        return self.key

    def read_one(self, line: tuple[Hashable, pd.Series]):
        index = line[0]
        symbol = line[1]["rockflow"]
        if not (hash(index) % self.sharding == self.partition):
            return
        self.log.info(f"process {symbol} in partition [{self.partition}]")
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
    def symbols(self) -> pd.DataFrame:
        return pd.read_csv(self.get_object(self.symbol_key), index_col=False)

    def merge_data(self):
        with Pool(self.pool_size) as pool:
            result = pool.map(
                lambda x: self.read_one(x), self.symbols.iterrows()
            )
            return result

    def get_symbol_key(self, symbol):
        return os.path.join(self.from_key, symbol + '.json')

    def save_key(self, key):
        file_name = f"{self.snakecase_class_name}_part_{self.partition}_of_{self.sharding}.json"
        return os.path.join(self.key + '_' + snakecase(key), file_name)

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
        with Pool(self.pool_size) as pool:
            pool.map(
                lambda x: self.put_object(self.save_key(x[0]), x[1]), self.content
            )


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


def yahoo_task_partition(shards, key, mysql_conn_id, upstream):
    yahoo = YahooBatchOperator(
        from_key="{{ task_instance.xcom_pull('" + upstream.task_id + "') }}",
        key=key,
        pool_size=1,
    )

    for i in range(shards):
        yahoo_extract = YahooExtractOperator(
            task_id=f"yahoo_extract_{i}",
            from_key="symbol_download_yahoo",
            key=key,
            symbol_key="{{ task_instance.xcom_pull('" + upstream.task_id + "') }}",
            partition=i,
            sharding=shards
        )

        summary_detail_mysql = SummaryDetailImportOperator(
            task_id=f"summary_detail_mysql_{i}",
            oss_source_key=yahoo_extract.save_key("SummaryDetail"),
            mysql_table='flow_ticker_summary_detail',
            mysql_conn_id=mysql_conn_id
        )

        yahoo_extract.set_upstream(upstream)
        yahoo_extract.set_upstream(yahoo)
        yahoo_extract.set_downstream(summary_detail_mysql)

    return yahoo

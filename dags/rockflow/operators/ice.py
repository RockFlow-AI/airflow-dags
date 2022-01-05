from datetime import datetime
from typing import Optional

import pandas as pd

from rockflow.common.pandas_helper import map_frame
from rockflow.operators.mysql import OssToMysqlOperator

SYMBOL_COLUMN = "Ticker Code (AS19)"
EXCHANGE_COLUMN = "Exchange of Quotation (-F3)"
NASDAQ_EXCHANGE = "NASDAQ"
HKEX_EXCHANGE = "S.E. HONG KONG LTD (HKEX)"
DATE_COLUMN = "Price Change Date - Subset 1 (\"FA1)"


class DailyHistoryImportOperator(OssToMysqlOperator):
    def __init__(self, **kwargs) -> None:
        if 'index_col' not in kwargs:
            kwargs['index_col'] = "id"
        if 'mapping' not in kwargs:
            kwargs['mapping'] = {
                SYMBOL_COLUMN: "symbol",
                DATE_COLUMN: "begin",
                "Opening Trade Price Subset-7 ($FS28)": "open",
                "Price B Sub-Set 3 ($FS3)": "high",
                "Price A Sub-Set 3 ($FS2)": "low",
                "Price A Sub-Set 1 ($F4)": "close",
                "Actual Volume (|FS26)": "volume",  # TODO(daijunkai) 需要排查解析失败原因目前看数据有0开头可能无法判读数据类型
                "id": "id",
            }
        super().__init__(**kwargs)

    def transform(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        def formart_symbol(row):
            symbol = row[SYMBOL_COLUMN]
            if row[EXCHANGE_COLUMN] == HKEX_EXCHANGE:
                if len(symbol) > 0:
                    return "%05d.HK" % int(symbol)
            return symbol

        def format_date(v):
            return datetime.strptime(v, "%Y%m%d").strftime("%Y-%m-%d")

        def format_timestamp(v):
            return int(datetime.strptime(v, "%Y-%m-%d").timestamp() * 1000)

        def format_id(row):
            return "%s|%d" % (row[SYMBOL_COLUMN], format_timestamp(row[DATE_COLUMN]))

        df = df.apply(lambda row: row.str.strip(), axis=1)
        df = df[(df[SYMBOL_COLUMN].notnull()) &
                (df[SYMBOL_COLUMN] != '') &
                (df[DATE_COLUMN].notnull()) &
                (df[DATE_COLUMN] != '') &
                ((df[EXCHANGE_COLUMN] != NASDAQ_EXCHANGE) | (df[EXCHANGE_COLUMN] == HKEX_EXCHANGE))]
        df[SYMBOL_COLUMN] = df.apply(lambda row: formart_symbol(row), axis=1)
        df[DATE_COLUMN] = df[DATE_COLUMN].apply(lambda row: format_date(row))
        df['id'] = df.apply(lambda row: format_id(row), axis=1)
        print(df)
        result = map_frame(df, self.mapping)
        print(result)
        result.set_index(self.index_col, inplace=True)
        return result

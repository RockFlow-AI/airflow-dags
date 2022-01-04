from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from rockflow.common.rename import new_symbol_with_market
from rockflow.operators.mysql import OssBatchToMysqlOperator


def date_to_timestamp(date_str):
    return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000)


def get_symbol(key):
    def split_name(file_name):
        split_res = file_name.split('-')
        if len(split_res) < 2:
            return ""
        return split_res[1]

    return new_symbol_with_market(split_name(Path(key).stem))


class HistoryImportOperator(OssBatchToMysqlOperator):
    def __init__(self, **kwargs) -> None:
        if 'mapping' not in kwargs:
            kwargs['mapping'] = {
                "Date": "begin",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        super().__init__(**kwargs)

    def transform(self, obj, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        def change_date_to_timestamp(obj, row):
            return "%s|%d" % (get_symbol(obj.key), date_to_timestamp(row['begin']))

        result = super().transform(obj, df)
        if result.empty:
            result['id'] = None
            result['symbol'] = None
        else:
            result['id'] = result.apply(
                lambda row: change_date_to_timestamp(obj, row), axis=1)
            result['symbol'] = get_symbol(obj.key)

        self.log.info(f"{result}")
        result.set_index("id", inplace=True)
        return result


class HistoryImportOperatorDebug(HistoryImportOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def iterator(self):
        from itertools import islice
        return islice(super().iterator(), 10)

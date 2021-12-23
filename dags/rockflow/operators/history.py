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
        super().__init__(**kwargs)

    def transform(self, obj, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        def change_date_to_timestamp(obj, row):
            return "%s|%d" % (get_symbol(obj.key), date_to_timestamp(row['begin']))

        result = super().transform(obj, df)
        result['id'] = result.apply(lambda row: change_date_to_timestamp(obj, row), axis=1)

        self.log.info(f"{result[:10]}")
        return result.index.rename("id")


class HistoryImportOperatorDebug(HistoryImportOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def iterator(self):
        from itertools import islice
        return islice(super().iterator(), 10)

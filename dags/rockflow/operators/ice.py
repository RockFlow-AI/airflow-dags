from typing import Optional

import pandas as pd

from rockflow.common.pandas_helper import map_frame
from rockflow.operators.mysql import OssToMysqlOperator


class DailyHistoryImportOperator(OssToMysqlOperator):
    def __init__(self, **kwargs) -> None:
        if 'index_col' not in kwargs:
            kwargs['index_col'] = "symbol"
        if 'mapping' not in kwargs:
            kwargs['mapping'] = {
                "Ticker Code (AS19)": "symbol",
                "Price Change Date - Subset 1 (\"FA1)": "begin",
                "Opening Trade Price Subset-7 ($FS28)": "open",
                "Price B Sub-Set 3 ($FS3)": "high",
                "Price A Sub-Set 3 ($FS2)": "low",
                "Price A Sub-Set 1 ($F4)": "close",
                "Actual Volume (|FS26)": "volume",
            }
        super().__init__(**kwargs)

    def transform(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        self.log.info(f"{df[:10]}")
        result = map_frame(df, self.mapping)
        self.log.info(f"{result[:10]}")
        result.set_index(self.index_col, inplace=True)
        return result

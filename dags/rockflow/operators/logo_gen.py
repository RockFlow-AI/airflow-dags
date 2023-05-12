import json
from multiprocessing.pool import ThreadPool as Pool
from typing import Any, Hashable

import oss2
import pandas as pd

from rockflow.operators.const import DEFAULT_POOL_SIZE
from rockflow.operators.oss import OSSOperator


class LogoImportOperator(OSSOperator):
    def __init__(self,
                 from_key: str,
                 avatar_bucket_name: str,
                 pool_size: int = DEFAULT_POOL_SIZE,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.from_key = from_key
        self.avatar_bucket_name = avatar_bucket_name
        self.pool_size = pool_size

    @property
    def avatar_bucket(self) -> oss2.api.Bucket:
        return self.oss_hook.get_bucket(self.avatar_bucket_name)

    @property
    def symbols(self) -> pd.DataFrame:
        result = pd.DataFrame.from_dict(
            json.loads(
                self.get_object(self.from_key).read()
            ),
            orient='index'
        )
        result.index.rename("symbol", inplace=True)
        return result

    def src_file(self, line: pd.Series) -> str:
        from pypinyin import pinyin, Style

        symbol = str(line["symbol"])
        name_en = str(line["name_en"])
        name_cn = str(line["name_cn"])

        symbol_file = self.oss_src(symbol)
        if self.object_exists_(self.avatar_bucket, symbol_file):
            return symbol_file
        result = symbol[0:1]
        if name_en:
            result = name_en[0:1]
        elif name_cn:
            result = name_cn[0:1]
        return self.oss_src(
            f"_{pinyin(result, style=Style.FIRST_LETTER)[0][0][0:1].upper()}"
        )

    def dest_file(self, line: pd.Series) -> str:
        return self.oss_dest(line["symbol"])

    def oss_src(self, file):
        return f"company/source/{file}.jpg"

    def oss_dest(self, file):
        return f"company/public/{file}.jpg"

    def save_one(self, line: tuple[Hashable, pd.Series]):
        index = line[0]
        symbol = line[1]
        self.log.info(f"index: {index}, symbol: {symbol}")
        self.copy_object_(self.avatar_bucket, self.src_file(symbol), self.dest_file(symbol))

    def execute(self, context: Any):
        self.log.info(f"symbol: {self.symbols}")
        with Pool(self.pool_size) as pool:
            pool.map(
                lambda x: self.save_one(x), self.symbols.iterrows()
            )

from typing import Optional

import pandas as pd

from rockflow.common.downloader import Downloader


class HKEX(Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def url(self):
        return "https://www.hkex.com.hk/chi/services/trading/securities/securitieslists/ListOfSecurities_c.xlsx"

    @property
    def type(self):
        return "xlsx"

    def to_tickers(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        result = pd.DataFrame()
        result['raw'] = df.iloc[:, 0]
        result['symbol'] = result['raw'].apply(
            lambda x: "%05d" % x
        )
        result['yahoo'] = result['raw'].apply(
            lambda x: "%04d.HK" % x
        )
        result['futu'] = result['raw'].apply(
            lambda x: "%05d-HK" % x
        )
        result['market'] = pd.Series(["HK" for _ in range(len(result.index))])
        return result

    def to_df(self, fp) -> pd.DataFrame:
        return pd.read_excel(
            fp,
            sheet_name='ListOfSecurities',
            header=[2],
            engine='openpyxl',
        )

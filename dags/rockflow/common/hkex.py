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

    def class_filter(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        return df[
            (df['分類'] == '股本') |
            (df['分類'] == '房地產投資信託基金')
        ]

    def format_symbol(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        result = pd.DataFrame()
        result['raw'] = df.iloc[:, 0]
        result['symbol'] = result['raw'].apply(
            lambda x: "%05d" % x
        )
        result['rockflow'] = result['raw'].apply(
            lambda x: "%05d.HK" % x
        )
        result['yahoo'] = result['raw'].apply(
            lambda x: "%04d.HK" % x
        )
        result['futu'] = result['raw'].apply(
            lambda x: "%05d-HK" % x
        )
        result['market'] = "HK"
        return result

    def to_tickers(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        return self.format_symbol(self.class_filter(df))

    def to_df(self, fp) -> pd.DataFrame:
        return pd.read_excel(
            fp,
            sheet_name='ListOfSecurities',
            header=[2],
            engine='openpyxl',
        )

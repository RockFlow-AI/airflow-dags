import warnings
from io import BytesIO
from typing import Optional

import pandas as pd

from rockflow.common.downloader import Downloader


class SZSE(Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stock_type(self):
        raise NotImplementedError()

    @property
    def url(self):
        return "http://www.szse.cn/api/report/ShowReport"

    @property
    def type(self):
        return "xlsx"

    @property
    def params(self):
        return {
            "SHOWTYPE": "xlsx",
            "CATALOGID": "1110",
            "TABKEY": self.stock_type,
        }

    def to_df(self, fp) -> pd.DataFrame:
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            return pd.read_excel(
                BytesIO(fp),
                engine="openpyxl",
            )

    def to_tickers(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        result = pd.DataFrame()
        result['raw'] = df.iloc[:, 4]
        result['rockflow'] = result['raw'].apply(
            lambda x: "%05d.SZ" % x
        )
        result['yahoo'] = result['raw'].apply(
            lambda x: "%06d.SZ" % x
        )
        result['ice'] = result['raw'].apply(
            lambda x: "%05d.SZ" % x
        )
        result['futu'] = result['raw'].apply(
            lambda x: "%06d-SZ" % x
        )
        result['market'] = "SZ"
        return result


class SZSE1(SZSE):
    """
    A股
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stock_type(self):
        return "tab1"


class SZSE2(SZSE):
    """
    B股
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stock_type(self):
        return "tab2"


class SZSE3(SZSE):
    """
    CDR列表
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stock_type(self):
        return "tab3"


class SZSE4(SZSE):
    """
    A+B股列表
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stock_type(self):
        return "tab4"

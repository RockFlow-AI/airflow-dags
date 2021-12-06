from rockflow.common.downloader import Downloader
from rockflow.common.header import user_agent
# from io import StringIO
#
# import pandas as pd
#
# from exchange import Exchange
# from utils import user_agent


class SSE(Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stock_type(self):
        raise NotImplementedError()

    @property
    def url(self):
        return "http://query.sse.com.cn/security/stock/downloadStockListFile.do"

    @property
    def type(self):
        return "csv"

    @property
    def params(self):
        return {
            "csrcCode": "",
            "stockCode": "",
            "areaName": "",
            "stockType": self.stock_type,
        }

    @property
    def headers(self):
        return {
            "Host": "query.sse.com.cn",
            "Connection": "keep-alive",
            "Accept": "*/*",
            "Origin": "http://www.sse.com.cn",
            "Referer": "http://www.sse.com.cn/assortment/stock/list/share/",
            "Accept-Encoding": "gzip:deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "User-Agent": user_agent,
        }

    # def _to_df(self) -> pd.DataFrame:
    #     return pd.read_csv(
    #         StringIO(self.oss().read().decode('gb18030')),
    #         sep='\t'
    #     )
    #
    # def _to_tickers(self) -> pd.DataFrame:
    #     result = pd.DataFrame()
    #     result['raw'] = self.csv_df().iloc[:, 0]
    #     result['symbol'] = result['raw'].astype(str)
    #     result['yahoo'] = result['raw'].apply(
    #         lambda x: "%d.SS" % x
    #     )
    #     result['futu'] = result['raw'].apply(
    #         lambda x: "%d-SH" % x
    #     )
    #     result['market'] = pd.Series(["SH" for _ in range(len(result.index))])
    #     return result


class SSE1(SSE):
    """
    A股
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stock_type(self):
        return "1"


class SSE2(SSE):
    """
    B股
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stock_type(self):
        return "2"


class SSE8(SSE):
    """
    科创板
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stock_type(self):
        return "8"

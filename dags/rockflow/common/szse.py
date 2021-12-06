from rockflow.common.downloader import Downloader
# from rockflow.common.header import user_agent

# import warnings
# from io import BytesIO
#
# import pandas as pd
#
# from exchange import Exchange


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

    # def _to_df(self) -> pd.DataFrame:
    #     with warnings.catch_warnings(record=True):
    #         warnings.simplefilter("always")
    #         return pd.read_excel(
    #             BytesIO(self.oss().read()),
    #             engine="openpyxl",
    #         )
    #
    # def _to_tickers(self) -> pd.DataFrame:
    #     result = pd.DataFrame()
    #     result['raw'] = self.csv_df().iloc[:, 5]
    #     result['symbol'] = result['raw'].apply(
    #         lambda x: "%06d" % x
    #     )
    #     result['yahoo'] = result['raw'].apply(
    #         lambda x: "%06d.SZ" % x
    #     )
    #     result['futu'] = result['raw'].apply(
    #         lambda x: "%06d-SZ" % x
    #     )
    #     result['market'] = pd.Series(["SZ" for _ in range(len(result.index))])
    #     return result


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

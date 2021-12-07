import pandas as pd

# from rockflow.common.hkex import HKEX
# from rockflow.common.nasdaq import Nasdaq
from rockflow.operators.oss import OSSOperator


class Tickers:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        # self.exchange_list = [
        #     Nasdaq(*args, **kwargs),
        #     HKEX(*args, **kwargs),
        #     # SSE1(*args, **kwargs),
        #     # SZSE1(*args, **kwargs),
        # ]

    def merge(self, key_list):
        tickers = None
        exchange_list = []
        # for exchange in self.exchange_list:
        #     exchange_tickers = exchange.to_tickers()  # key
        #     if tickers is None:
        #         tickers = exchange_tickers
        #     else:
        #         tickers = pd.concat([tickers, exchange_tickers], axis=0, ignore_index=True)
        for key in key_list:
            exchange_list.append(pd.read_csv(OSSOperator.get_object(key)))
        for exchange in exchange_list:
            exchange_tickers = exchange.to_tickers()  # key
            if tickers is None:
                tickers = exchange_tickers
            else:
                tickers = pd.concat([tickers, exchange_tickers], axis=0, ignore_index=True)
        return tickers
        # self.bucket.put_object(key, tickers.to_csv())

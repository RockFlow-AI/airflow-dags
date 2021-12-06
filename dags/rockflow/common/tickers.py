import pandas as pd

from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq


class Tickers:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.exchange_list = [
            Nasdaq(*args, **kwargs),
            HKEX(*args, **kwargs),
            # SSE1(*args, **kwargs),
            # SZSE1(*args, **kwargs),
        ]

    def merge(self, key):
        tickers = None
        for exchange in self.exchange_list:
            exchange_tickers = exchange.to_tickers()  # key
            if tickers is None:
                tickers = exchange_tickers
            else:
                tickers = pd.concat([tickers, exchange_tickers], axis=0, ignore_index=True)
        # self.bucket.put_object(key, tickers.to_csv())

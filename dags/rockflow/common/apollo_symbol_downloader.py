import json
from io import BytesIO
from typing import Optional

import pandas as pd

from rockflow.common.downloader import Downloader
from rockflow.operators.const import APOLLO_HOST, APOLLO_PORT


class ApolloSymbolDownloader(Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def market(self):
        raise NotImplementedError()

    @property
    def url(self):
        return f"http://{APOLLO_HOST}:{APOLLO_PORT}/configs/wing/PROD/server.symbols.{self.market}"

    @property
    def type(self):
        return "json"

    @property
    def params(self):
        return {}

    @property
    def headers(self):
        return {}

    @property
    def proxy(self):
        return None

    @property
    def timeout(self):
        return 10

    def to_df(self, fp) -> pd.DataFrame:
        response = json.load(BytesIO(fp))
        symbols = response.get('configurations').get(f'flow.feed.tick.realtime.subscriptions.{self.market}.symbols',
                                                     response.get('configurations'))
        return pd.DataFrame(symbols.split(','), columns=['symbol'])


class ApolloUS(ApolloSymbolDownloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def market(self):
        return 'nasdaq'

    def to_tickers(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        def rockflow_symbol(raw: str):
            return raw.strip() \
                .replace("^", "-") \
                .replace("/", ".").upper()

        def yahoo_symbol(raw: str):
            return raw.strip() \
                .replace("^", "-P") \
                .replace(".", "-") \
                .replace("/", "-").upper()

        def ice_symbol(raw: str):
            return raw.strip() \
                .replace("^", ".PR") \
                .replace("/", ".").upper()

        def futu_symbol(raw: str):
            return "%s-US" % raw.strip() \
                .replace("^", "-") \
                .replace("/", ".").upper()

        result = pd.DataFrame()
        result['raw'] = df['symbol']
        result['rockflow'] = result['raw'].apply(
            lambda x: rockflow_symbol(x)
        )
        result['yahoo'] = result['raw'].apply(
            lambda x: yahoo_symbol(x)
        )
        result['ice'] = result['raw'].apply(
            lambda x: ice_symbol(x)
        )
        result['futu'] = result['raw'].apply(
            lambda x: futu_symbol(x)
        )
        result['market'] = "US"
        return result

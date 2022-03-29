from typing import Optional

import pandas as pd

from rockflow.common.apollo_symbol_downloader import ApolloSymbolDownloader


class ApolloNasdaq(ApolloSymbolDownloader):
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

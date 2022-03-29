from typing import Optional

import pandas as pd

from rockflow.common.apollo_symbol_downloader import ApolloSymbolDownloader


class ApolloHKEX(ApolloSymbolDownloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def market(self):
        return 'hk'

    def format_symbol(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        result = pd.DataFrame()
        result['raw'] = df['symbol']
        result['rockflow'] = result['raw']
        result['yahoo'] = result['raw'].apply(
            lambda x: x[1:]
        )
        result['ice'] = result['raw']
        result['futu'] = result['raw'].strip().replace(".", "-")
        result['market'] = "HK"
        return result

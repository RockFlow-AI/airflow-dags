import json
from io import BytesIO

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

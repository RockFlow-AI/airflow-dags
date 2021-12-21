import json
from io import BytesIO
from typing import Optional

import pandas as pd

from rockflow.common.downloader import Downloader


class Nasdaq(Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def url(self):
        return "https://api.nasdaq.com/api/screener/stocks"

    @property
    def type(self):
        return "json"

    @property
    def params(self):
        return {
            'tableonly': 'false',
            'limit': 0,
            'offset': 0,
            'download': 'true',
        }

    @property
    def headers(self):
        return {
            'authority': 'api.nasdaq.com',
            'accept': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
            'origin': 'https://www.nasdaq.com',
            'sec-fetch-site': 'same-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.nasdaq.com/',
            'accept-language': 'en-US,en;q=0.9',
        }

    @property
    def proxy(self):
        return None

    @property
    def timeout(self):
        return 60

    def to_tickers(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        result = pd.DataFrame()
        result['raw'] = df['symbol']
        result['symbol'] = result['raw'].astype(str)
        result['rockflow'] = result['raw'].apply(
            lambda x: x.strip().replace("^", "-P").replace("/", "-").upper()
        )
        result['yahoo'] = result['raw'].apply(
            lambda x: x.strip().replace("^", "-P").replace("/", "-").upper()
        )
        result['futu'] = result['yahoo'].apply(
            lambda x: "%s-US" % x
        )
        result['market'] = pd.Series(["US" for _ in range(len(result.index))])
        return result

    def to_df(self, fp) -> pd.DataFrame:
        response = json.load(BytesIO(fp))
        return pd.DataFrame(
            response.get('data').get(
                'table', response.get('data')
            ).get('rows')
        )

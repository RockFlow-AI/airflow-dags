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
        """
        强制不走代理，失败原因未知，需要工具测试下
        """
        return None

    @property
    def timeout(self):
        return 60

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

    def to_df(self, fp) -> pd.DataFrame:
        response = json.load(BytesIO(fp))
        return pd.DataFrame(
            response.get('data').get(
                'table', response.get('data')
            ).get('rows')
        )

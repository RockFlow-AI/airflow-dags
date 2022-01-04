from io import BytesIO

import pandas as pd
from rockflow.common.downloader import Downloader


class MIC(Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def url(self):
        return "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"

    @property
    def type(self):
        return "csv"

    def to_df(self) -> pd.DataFrame:
        r = self.get()
        return pd.read_csv(
            BytesIO(r.content),
            encoding=r.encoding,
        )

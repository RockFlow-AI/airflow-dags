from rockflow.common.downloader import Downloader


# from rockflow.common.header import user_agent
# from io import BytesIO
#
# import pandas as pd
#
# from .csvd import CSV
#

class MIC(Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def url(self):
        return "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"

    @property
    def type(self):
        return "csv"

    # def _to_df(self) -> pd.DataFrame:
    #     r = self._get()
    #     return pd.read_csv(
    #         BytesIO(r.content),
    #         encoding=r.encoding,
    #     )

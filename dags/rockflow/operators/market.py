from rockflow.common.mic import MIC
from rockflow.operators.downloader import DownloadOperator
from rockflow.operators.mysql import OssToMysqlOperator


class MicDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return MIC


class MarketImportOperator(OssToMysqlOperator):
    def __init__(self, **kwargs) -> None:
        if 'index_col' not in kwargs:
            kwargs['index_col'] = "mic"
        if 'mapping' not in kwargs:
            kwargs['mapping'] = {
                "MIC": "mic",
                "OPERATING MIC": "operating_mic",
                "NAME-INSTITUTION DESCRIPTION": "name",
                "ACRONYM": "acronym",
                "CITY": "city",
                "COUNTRY": "country",
                "WEBSITE": "website",
            }
        super().__init__(**kwargs)

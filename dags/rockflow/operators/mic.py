from rockflow.common.mic import MIC
from rockflow.operators.downloader import DownloadOperator


class MicDownloadOperator(DownloadOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def downloader_cls(self):
        return MIC

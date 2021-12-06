from typing import Optional

from airflow.models import BaseOperator

from rockflow.common.hkex import HKEX
from rockflow.common.nasdaq import Nasdaq


class NasdaqSymbolDownloadOperator(BaseOperator):
    def __init__(
            self,
            proxy: Optional[dict] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.proxy = proxy

    def execute(self, context):
        nasdaq = Nasdaq(proxy=self.proxy)
        nasdaq._get()


class HkexSymbolDownloadOperator(BaseOperator):
    def __init__(
            self,
            proxy: Optional[dict] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.proxy = proxy

    def execute(self, context):
        hkex = HKEX(proxy=self.proxy)
        hkex._get()

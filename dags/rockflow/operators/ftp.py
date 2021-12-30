from typing import Any

from airflow.providers.ftp.hooks.ftp import FTPHook

from rockflow.operators.oss import OSSOperator


class FtptToOssOperator(OSSOperator):
    def __init__(
            self,
            prefix: str,
            work_dir: str,
            ftp_conn_id: str = 'ftp_default',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.prefix = prefix
        self.work_dir = work_dir
        self.ftp_conn_id = ftp_conn_id
        self.ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)

    def list_directory(self):
        self.ftp_hook.list_directory(self.work_dir)

    def execute(self, context: Any):
        print(self.list_directory())

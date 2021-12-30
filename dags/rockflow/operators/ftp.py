import os
from typing import Any

from airflow.providers.ssh.hooks.ssh import SSHHook

from rockflow.operators.oss import OSSOperator


class SftptToOssOperator(OSSOperator):
    def __init__(
            self,
            prefix: str,
            work_dir: str,
            ssh_conn_id: str = 'ssh_default',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.prefix = prefix
        self.work_dir = work_dir
        self.ssh_conn_id = ssh_conn_id
        self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

    @property
    def sftp_client(self):
        return self.ssh_hook.get_conn().open_sftp()

    def execute(self, context: Any):
        sftp_path = os.path.join(self.work_dir, "K16D75_20211229.SRV")
        with self.sftp_client.file(sftp_path, mode='rb') as data:
            self.put_object("ice_sftp_sync/bak", data)

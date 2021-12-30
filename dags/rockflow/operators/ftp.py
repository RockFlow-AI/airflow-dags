import os
from tempfile import NamedTemporaryFile
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
        for file in self.sftp_client.listdir_attr(self.work_dir):
            filename = file.filename
            print(filename)
            sftp_path = os.path.join(self.work_dir, filename)
            dest_path = os.path.join(self.prefix, filename)
            with NamedTemporaryFile("w") as f:
                self.sftp_client.get(sftp_path, f.name)
                self.put_object(dest_path, f.name)

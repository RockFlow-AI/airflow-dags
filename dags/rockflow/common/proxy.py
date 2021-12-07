import os
from typing import Optional

from airflow.models import Variable


def is_docker():
    path = '/proc/self/cgroup'
    return (
            os.path.exists('/.dockerenv') or
            os.path.isfile(path) and any('docker' in line for line in open(path))
    )


class Proxy:
    def __init__(self, host, port) -> None:
        self.host = host
        self.port = port

    @property
    def proxies(self) -> Optional[dict]:
        proxies = {
            'http': f"http://{self.host}:{self.port}",
            'https': f"http://{self.host}:{self.port}"
        }
        return proxies


def default_proxy() -> Optional[dict]:
    if not is_docker():
        return Proxy("127.0.0.1", "7890").proxies
    else:
        return Proxy(Variable.get("PROXY_URL"), Variable.get("PROXY_PORT")).proxies

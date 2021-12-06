from typing import Optional

from airflow.models import Variable


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
    return Proxy(Variable.get("PROXY_URL"), Variable.get("PROXY_PORT")).proxies

from typing import Optional


class Proxy:
    def __init__(self, host, port) -> None:
        self.host = host
        self.port = port

    @property
    def proxies(self) -> Optional[dict]:
        proxies = {
            "http://": f"http://{self.host}:{self.port}",
            "https://": f"http://{self.host}:{self.port}"
        }
        return proxies


def local_proxy() -> Optional[dict]:
    return Proxy("127.0.0.1", "7890").proxies

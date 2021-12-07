import unittest
from typing import Optional

from rockflow.common.nasdaq import Nasdaq


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
    return Proxy("127.0.0.1", "7890").proxies


class TestNasdaq(unittest.TestCase):
    def test_nasdaq(self):
        # python -m unittest test_nasdaq.TestNasdaq.test_nasdaq
        nasdaq = Nasdaq(proxy=default_proxy())
        print(nasdaq.get().content)


if __name__ == '__main__':
    unittest.main()

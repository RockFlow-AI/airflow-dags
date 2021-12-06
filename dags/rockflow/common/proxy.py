from airflow.models import Variable


class Proxy:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    @property
    def proxies(self):
        proxies = {
            'http': f"http://{self.host}:{self.port}",
            'https': f"http://{self.host}:{self.port}"
        }
        return proxies


def default_proxy():
    return Proxy(Variable.get("PROXY_URL"), Variable.get("PROXY_PORT")).proxies

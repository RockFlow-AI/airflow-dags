from airflow.models import Variable

from rockflow.common.proxy import Proxy

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"

user_agent_headers = {
    "User-Agent": user_agent,
}


def default_proxy():
    return Proxy(Variable.get("PROXY_URL"), Variable.get("PROXY_PORT")).proxies

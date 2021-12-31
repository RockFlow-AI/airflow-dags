from airflow.models import Variable

from rockflow.common.proxy import Proxy

DEFAULT_POOL_SIZE = Variable.get("THREAD_POOL_SIZE", 24)
DEFAULT_PROXY = Proxy(Variable.get("PROXY_URL"),
                      Variable.get("PROXY_PORT")).proxies
DEFAULT_REGION = Variable.get("REGION")
DEFAULT_BUCKET_NAME = Variable.get("BUCKET_NAME")

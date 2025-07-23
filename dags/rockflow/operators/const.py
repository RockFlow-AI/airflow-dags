from airflow.models import Variable

from rockflow.common.proxy import Proxy

DEFAULT_POOL_SIZE = int(Variable.get("THREAD_POOL_SIZE", "24"))
DEFAULT_PROXY = Proxy(Variable.get("PROXY_URL"),
                      Variable.get("PROXY_PORT")).proxies
DEFAULT_REGION = Variable.get("REGION")
DEFAULT_BUCKET_NAME = Variable.get("BUCKET_NAME")
AVATAR_BUCKET_NAME = Variable.get("BUCKET_NAME_AVATAR")
APOLLO_HOST = Variable.get("APOLLO_HOST")
APOLLO_PORT = Variable.get("APOLLO_PORT")
GLOBAL_DEBUG = (Variable.get("DEBUG", "false") == "true")
CONTAINER_REPO = Variable.get("CONTAINER_REPO")
DW_CONNECTOR_VERSION = Variable.get("DW_CONNECTOR_VERSION")
MQ_CONNECTOR_VERSION = Variable.get("MQ_CONNECTOR_VERSION")
DW_RAW_VERSION = Variable.get("DW_RAW_VERSION")
OPTION_ALGO_VERSION = Variable.get("OPTION_ALGO_VERSION")
OPTION_ALGO_SHARDS = int(Variable.get("OPTION_ALGO_SHARDS", 10))
SPIDER_VERSION = Variable.get("SPIDER_VERSION")

from airflow.models import Variable

from rockflow.common.proxy import Proxy

MERGE_CSV_KEY = 'airflow-symbol-csv-merge/merge.csv'

DEFAULT_PROXY = Proxy(Variable.get("PROXY_URL"), Variable.get("PROXY_PORT")).proxies
DEFAULT_REGION = Variable.get("REGION")
DEFAULT_BUCKET_NAME = Variable.get("BUCKET_NAME")

from airflow.models import Variable

DEFAULT_POOL_SIZE = Variable.get("THREAD_POOL_SIZE", 24)

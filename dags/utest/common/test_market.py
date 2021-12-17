import unittest

from airflow.models import DAG
from rockflow.dags.const import *
from rockflow.operators.market import MicDownloadOperator


class Test(unittest.TestCase):
    def test_market(self):
        with DAG("market_download", default_args=DEFAULT_DEBUG_ARGS) as mic:
            mic = MicDownloadOperator(
                key=mic.dag_id,
                region=DEFAULT_REGION,
                bucket_name=DEFAULT_BUCKET_NAME,
                proxy=DEFAULT_PROXY
            )
            mic.execute("")


if __name__ == '__main__':
    unittest.main()

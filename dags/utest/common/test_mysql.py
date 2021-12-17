import unittest
from unittest import mock
from dotenv import load_dotenv, find_dotenv

from airflow.hooks.base import BaseHook
from airflow.models import Connection, Variable

load_dotenv(find_dotenv(), override=True)

class Test(unittest.TestCase):
    def test_env(self):
        assert "rockflow-data-dev" == Variable.get("bucket_name")
        with mock.patch.dict("os.environ", AIRFLOW_VAR_KEY="env-value"):
            assert "env-value" == Variable.get("key")

    def test(self):
        test_conn = BaseHook.get_connection(conn_id="my_conn")
        assert "cat" == test_conn.login
        test_conn_2 = BaseHook.get_connection(conn_id="elasticsearch_default")
        assert "es-test.es.rockflow.tech" == test_conn_2.host
        test_conn_3 = BaseHook.get_connection(conn_id="flow-portfolio-service")
        assert "10.131.18.44" == test_conn_3.host
        conn = Connection(
            conn_type="gcpssh",
            login="cat",
            host="conn-host",
        )
        conn_uri = conn.get_uri()
        with mock.patch.dict("os.environ", AIRFLOW_CONN_MY_CONN=conn_uri):
            test_conn = BaseHook.get_connection(conn_id="my_conn")
            assert "cat" == test_conn.login


if __name__ == '__main__':
    unittest.main()

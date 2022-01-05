import unittest
from unittest import mock

from airflow.hooks.base import BaseHook
from airflow.models import Connection, Variable


class Test(unittest.TestCase):
    def test_mock_env(self):
        with mock.patch.dict("os.environ", AIRFLOW_VAR_KEY="env-value"):
            self.assertEqual("env-value", Variable.get("key"))

    def test_mock_connection(self):
        conn = Connection(
            conn_type="gcpssh",
            login="cat",
            host="conn-host",
        )
        conn_uri = conn.get_uri()
        with mock.patch.dict("os.environ", AIRFLOW_CONN_MY_CONN=conn_uri):
            test_conn = BaseHook.get_connection(conn_id="my_conn")
            self.assertEqual("cat", test_conn.login)

    def test_variable(self):
        self.assertEqual("rockflow-data-dev", Variable.get("bucket_name"))
        self.assertEqual("7890", Variable.get("proxy_port"))
        self.assertEqual("127.0.0.1", Variable.get("proxy_url"))
        self.assertEqual("cn-hongkong", Variable.get("region"))
        self.assertRaises(KeyError, lambda: Variable.get("not_exist"))

    def test_get_connection(self):
        conn = BaseHook.get_connection(conn_id="elasticsearch_default")
        self.assertEqual("es-test.es.rockflow.tech", conn.host)
        self.assertEqual(9200, conn.port)

        conn = BaseHook.get_connection(conn_id="flow-portfolio-service")
        self.assertEqual("10.131.18.44", conn.host)
        self.assertEqual(30847, conn.port)


if __name__ == '__main__':
    unittest.main()

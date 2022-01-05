import unittest

from rockflow.dags.const import MYSQL_CONNECTION_FLOW_TICKER
from rockflow.operators.futu import SinkFutuProfileDebug


class Test(unittest.TestCase):
    def test_market(self):
        sink_futu_profile_op = SinkFutuProfileDebug(
            oss_source_key="/Users/daijunkai/Downloads/join_map.json",
            mysql_table='flow_ticker_stock_profile',
            mysql_conn_id=MYSQL_CONNECTION_FLOW_TICKER
        )
        self.assertIsNone(sink_futu_profile_op.execute(""))


if __name__ == '__main__':
    unittest.main()

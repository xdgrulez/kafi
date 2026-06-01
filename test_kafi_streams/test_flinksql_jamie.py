from test_kafi_streams.test_flinksql_base import TestFlinkSqlBase, home_path_str
from test_kafi_streams.test_topologies import TestTopologies
from test_kafi_streams.test_generate import TestGenerate
from test_kafi_streams.test_base import TestBase

from kafi.kafi import Cluster

#

class TestFlinkSqlJamie(TestFlinkSqlBase, TestTopologies, TestGenerate, TestBase):
    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_kafi_streams/jamie/flinksql/jamie.sql"
        #
        source_storage = Cluster("local")
        #
        transaction_topic_str = transaction_source_str
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, transaction_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "flink_total"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, 100)
        #
        # self.assertEqual(len(self.updated_message_dict_list), 1)
        # self.assertEqual(self.updated_message_dict_list[0]["value"], {"sum": 0})

from test_streams._test_flinksql_base import TestFlinkSqlBase, home_path_str
from test_streams.test_base import TestBase

from kafi.kafi import Cluster

#

class TestFlinkSqlDatagen(TestFlinkSqlBase, TestBase):
    def test_datagen_1_join(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_streams/datagen/flinksql/1_join.sql"
        #
        source_storage = Cluster("local")
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, 100), (source_storage, customer_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "flink_1_join"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, 100)

    def test_datagen_2_joins(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        product_topic_str = "shoes"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_streams/datagen/flinksql/2_joins.sql"
        #
        source_storage = Cluster("local")
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, 100), (source_storage, customer_topic_str, 100), (source_storage, product_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "flink_2_joins"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, 100)

    def test_datagen_3_joins(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        product_topic_str = "shoes"
        order_topic_str = "shoe_orders"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_streams/datagen/flinksql/3_joins.sql"
        #
        source_storage = Cluster("local")
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, 100), (source_storage, customer_topic_str, 100), (source_storage, product_topic_str, 100), (source_storage, order_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "flink_3_joins"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, 100)

from test_kafi_streams.test_flinksql_base import TestFlinkSqlBase, home_path_str
from test_kafi_streams.test_generate import TestGenerate
from test_kafi_streams.test_base import TestBase, default_batch_size_int, default_steps_int

from kafi.kafi import Cluster

#

class TestFlinkSql(TestFlinkSqlBase, TestGenerate, TestBase):
    def test_datagen_1_join(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_kafi_streams/datagen/flinksql/1_join.sql"
        #
        source_storage = Cluster("local")
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, default_batch_size_int), (source_storage, customer_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "flink_1_join"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, default_steps_int)

    def test_datagen_2_joins(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        product_topic_str = "shoes"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_kafi_streams/datagen/flinksql/2_joins.sql"
        #
        source_storage = Cluster("local")
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, default_batch_size_int), (source_storage, customer_topic_str, default_batch_size_int), (source_storage, product_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "flink_2_joins"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, default_steps_int)

    def test_datagen_3_joins(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        product_topic_str = "shoes"
        order_topic_str = "shoe_orders"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_kafi_streams/datagen/flinksql/3_joins.sql"
        #
        source_storage = Cluster("local")
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, default_batch_size_int), (source_storage, customer_topic_str, default_batch_size_int), (source_storage, product_topic_str, default_batch_size_int), (source_storage, order_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "flink_3_joins"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, default_steps_int)

    def test_datagen_self_join_group_by(self):
        order_topic_str = "shoe_orders"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_kafi_streams/datagen/flinksql/self_join_group_by.sql"
        #
        source_storage = Cluster("local")
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, order_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "flink_self_join_group_by"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, default_steps_int)

    def test_datagen_self_join_group_by_debezium(self):
        order_topic_str = "shoe_orders_debezium"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_kafi_streams/datagen/flinksql/self_join_group_by_debezium.sql"
        #
        source_storage = Cluster("local")
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, order_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "flink_self_join_group_by_debezium"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, default_steps_int)

    #

    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        flinksql_sql_path_str = f"{home_path_str}/github/kafi/test_kafi_streams/jamie/flinksql/jamie.sql"
        #
        source_storage = Cluster("local")
        #
        transaction_topic_str = transaction_source_str
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, transaction_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "flink_total"
        #
        self.go(flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, 1000)
        #
        self.assertEqual(self.updated_record_any_list[-1]["value"], {"total": 0})

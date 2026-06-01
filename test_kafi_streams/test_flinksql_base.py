import json, subprocess, threading, time, unittest

from kafi.helpers import get

#

default_pack_function = json.dumps
default_unpack_function = json.loads

#

home_path_str = "/home/ralph"
# home_path_str = "/Users/m0724822"
flinksql_path_str = f"{home_path_str}/apps/flink-2.2.0"
flinksql_start_cluster_str = f"{flinksql_path_str}/bin/start-cluster.sh"
flinksql_stop_cluster_str = f"{flinksql_path_str}/bin/stop-cluster.sh"
flinksql_sql_client_path_str = f"{flinksql_path_str}/bin/sql-client.sh"
flinksql_url_str = "http://localhost:9081"

#

class TestFlinkSqlBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print("Test:", self._testMethodName)
        #
        self.source_str_values_int_dict = {}
        self.updated_value_any_list = {}
        self.deleted_value_any_list = {}
        self.generator_dict = {}

    def tearDown(self):
        subprocess.run("pgrep -f flink | xargs kill -9", shell=True)
        #
        print()
        print("---")
        print()
        #
        print(f"Inputs: {self.source_str_values_int_dict}")
        #
        print()
        print("---")
        print()
        #
        self.print_changes(self.updated_value_any_list, "Updates")
        #
        # print()
        # print("---")
        # print()
        # #
        # self.print_changes(self.deleted_value_any_list, "Deletes")
        #
        print()
        print("---")
        print()

    def print_changes(self, changed_value_any_list, changes_str):
        changed_serialized_value_any_list = [default_pack_function(value_any) for value_any in changed_value_any_list]
        changes_int = len(changed_serialized_value_any_list)
        unique_changes_int = len(set(changed_serialized_value_any_list))
        print(f"{changes_str}: {changes_int}")
        print()
        print(f"Unique: {unique_changes_int}")
        print()
        if changes_int > 6:
            print("First three:")
            for changed_serialized_value_any in changed_serialized_value_any_list[:3]: 
                print(default_unpack_function(changed_serialized_value_any))
            print()
            print("Last three:")
            for changed_serialized_value_any in changed_serialized_value_any_list[-3:]: 
                print(default_unpack_function(changed_serialized_value_any))
        elif changes_int > 0:
            print("Last:")
            print(default_unpack_function(changed_serialized_value_any_list[-1]))

    #

    def produce(self, storage_topic_str_batch_size_int_tuple_list, steps_int):
        for _ in range(steps_int):
            for storage, topic_str, batch_size_int in storage_topic_str_batch_size_int_tuple_list:
                    message_dict_list = self.generate(topic_str, batch_size_int)
                    #
                    producer = storage.producer(topic_str)
                    producer.produce_list(message_dict_list)
                    producer.close()
                    #
                    self.source_str_values_int_dict[topic_str] += batch_size_int

    #

    def process(self, flinksql_sql_path_str):
        start_cluster_completedProcess = subprocess.run([flinksql_start_cluster_str], capture_output=True, text=True)
        print(start_cluster_completedProcess.stdout)
        #
        sql_client_completedProcess = subprocess.run([flinksql_sql_client_path_str, "-f", flinksql_sql_path_str], capture_output=True, text=True)
        print(sql_client_completedProcess.stdout)

    #

    def stop_function(self):
        stop_cluster_completedProcess = subprocess.run([flinksql_stop_cluster_str], capture_output=True, text=True)
        print(stop_cluster_completedProcess.stdout)

    #

    def read(self, storage, topic_str):
        updates_int = storage.l(topic_str)[topic_str]
        #
        self.updates_int = updates_int
        #
        message_dict_list = storage.cat(topic_str, n=1)
        self.updated_value_any_list = message_dict_list

    #

    def stop(self, source_topic_str, batch_size_int, steps_int):
        try:
            jobs_response_dict = get(f"{flinksql_url_str}/jobs")
            job_dict_list = jobs_response_dict["jobs"]
            if len(job_dict_list) == 0:
                raise Exception("No job running.")
            elif len(job_dict_list) > 1:
                raise Exception("More than one jobs running.")
            #
            job_dict = job_dict_list[0]
            job_id_str = job_dict["id"]
            #
            job_response_dict = get(f"{flinksql_url_str}/jobs/{job_id_str}")
            vertice_dict_list = job_response_dict["vertices"]
            source_vertice_dict_list = [vertice_dict for vertice_dict in vertice_dict_list if vertice_dict["name"].startswith(f"Source: {source_topic_str}")]
            if not len(source_vertice_dict_list) == 1:
                raise Exception(f"Could not find source vertice for {source_topic_str}.")
            read_records_int = source_vertice_dict_list[0]["metrics"]["read-records"]
            #
            return read_records_int == batch_size_int * steps_int
        except Exception as e:
            print(e)
            return False
    
    #

    def go(self, flinksql_sql_path_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, steps_int):
        source_storage_topic_str_tuple_list = [(storage, topic_str) for storage, topic_str, _ in source_storage_topic_str_batch_size_int_tuple_list]
        #
        self.source_str_values_int_dict = {source_str: 0 for _, source_str in source_storage_topic_str_tuple_list}
        #
        for storage, topic_str in source_storage_topic_str_tuple_list:
            storage.recreate(topic_str)
        target_storage.recreate(target_topic_str)
        #
        for _, topic_str, _ in source_storage_topic_str_batch_size_int_tuple_list:
            self.init_generate(topic_str)
        #
        thread1 = threading.Thread(target=self.produce, args=(source_storage_topic_str_batch_size_int_tuple_list, steps_int))
        #
        thread2 = threading.Thread(target=self.process, args=(flinksql_sql_path_str, ))
        #
        thread1.start()
        thread2.start()
        #
        while True:
            if all(self.stop(topic_str, batch_size_int, steps_int) for _, topic_str, batch_size_int in source_storage_topic_str_batch_size_int_tuple_list):
                break
            #
            time.sleep(1)
        #
        self.stop_function()
        #
        thread1.join()
        thread2.join()
        #
        self.read(target_storage, target_topic_str)

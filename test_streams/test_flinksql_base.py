import json, subprocess, threading, time, unittest

from kafi.helpers import get

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
    async def asyncSetUp(self):
        print("Test:", self._testMethodName)

    async def asyncTearDown(self):
        print()
        print("---")
        print()
        #
        print(f"Inputs: {self.source_str_messages_int_dict}")
        #
        print()
        print("---")
        print()
        #
        print(f"Updates: {self.updates_int}")
        if self.updates_int > 0:
            print("First update:")
            print(json.dumps(self.updated_message_dict_list[0], indent=2))
        #
        print()
        print("---")
        print()

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
                    self.source_str_messages_int_dict[topic_str] += batch_size_int

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
        self.updated_message_dict_list = message_dict_list

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
        self.source_str_messages_int_dict = {source_str: 0 for _, source_str in source_storage_topic_str_tuple_list}
        #
        for storage, topic_str in source_storage_topic_str_tuple_list:
            storage.recreate(topic_str)
        target_storage.recreate(target_topic_str)
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

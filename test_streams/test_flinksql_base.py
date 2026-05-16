import json, subprocess, threading, time, unittest

#

flinksql_path_str = "/home/ralph/apps/flink-2.2.0"
flinksql_start_cluster_str = f"{flinksql_path_str}/bin/start-cluster.sh"
flinksql_stop_cluster_str = f"{flinksql_path_str}/bin/stop-cluster.sh"
flinksql_sql_client_path_str = f"{flinksql_path_str}/bin/sql-client.sh"

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
        updates_int = len(self.updated_message_dict_list)
        updated_message_json_str_list = [json.dumps(message_dict) for message_dict in self.updated_message_dict_list]
        unique_updates_int = len(set(updated_message_json_str_list))
        print(f"Updates: {updates_int}")
        print(f"Unique updates: {unique_updates_int}")
        if updates_int > 0:
            print("First update:")
            print(json.dumps(self.updated_message_dict_list[0], indent=2))
        #
        print()
        print("---")
        print()
        #
        # deletes_int = len(self.deleted_message_dict_list)
        # print(f"Deletes: {deletes_int}")
        # if deletes_int > 0:
        #     print("First delete:")
        #     print(json.dumps(self.updated_message_dict_list[0], indent=2))
        # #
        # print()
        # print("---")
        # print()

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
        message_dict_list = storage.l(topic_str)[topic_str]
        #
        self.updated_message_dict_list = message_dict_list

    #

    def stop(self, source_storage, source_topic_str, batch_size_int, group_str, steps_int):
        group_str_topic_str_offsets_dict_dict_dict = source_storage.group_offsets(group_str)
        if group_str not in group_str_topic_str_offsets_dict_dict_dict:
            return False
        topic_str_offsets_dict_dict = group_str_topic_str_offsets_dict_dict_dict[group_str]
        if source_topic_str not in topic_str_offsets_dict_dict:
            return False
        #
        offsets_dict = topic_str_offsets_dict_dict[source_topic_str]
        offset_int = offsets_dict[0]
        return offset_int >= steps_int * batch_size_int

    #

    def go(self, flinksql_sql_path_str, flinksql_group_str, source_storage_topic_str_batch_size_int_tuple_list, target_storage, target_topic_str, steps_int):
        source_storage_topic_str_tuple_list = [(storage, topic_str) for storage, topic_str, _ in source_storage_topic_str_batch_size_int_tuple_list]
        #
        for source_storage, topic_str in source_storage_topic_str_tuple_list:
            source_storage.grm(flinksql_group_str)
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
            if all(self.stop(storage, topic_str, batch_size_int, flinksql_group_str, steps_int) for storage, topic_str, batch_size_int in source_storage_topic_str_batch_size_int_tuple_list):
                break
            #
            time.sleep(0.1)
        #
        self.stop_function()
        #
        thread1.join()
        thread2.join()
        #
        self.read(target_storage, target_topic_str)

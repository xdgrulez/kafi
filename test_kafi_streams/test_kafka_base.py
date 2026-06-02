class TestKafkaBase:
    def produce(self, storage_topic_str_batch_size_int_tuple_list, steps_int, key_type, value_type):
        for _ in range(steps_int):
            for storage, topic_str, batch_size_int in storage_topic_str_batch_size_int_tuple_list:
                    value_any_list = self.generate(topic_str, batch_size_int)
                    for x in value_any_list:
                         print(x["value"])
                    #
                    producer = storage.producer(topic_str, key_type=key_type, value_type=value_type)
                    producer.produce_list(value_any_list)
                    producer.close()
                    #
                    self.source_str_values_int_dict[topic_str] += batch_size_int

    def read(self, storage, topic_str, key_type, value_type):
        value_any_list = storage.cat(topic_str, key_type=key_type, value_type=value_type)
        print("")
        for x in value_any_list:
             print(x["value"])
        #
        self.updates_int = len(value_any_list)
        #
        self.updated_value_any_list = value_any_list

class StorageProducer():
    def __init__(self, storage_obj, topic, **kwargs):
        self.storage_obj = storage_obj
        #
        self.topic_str = topic
        #
        (self.key_type_str, self.value_type_str) = storage_obj.get_key_value_type_tuple(**kwargs)
        #
        self.written_counter_int = 0

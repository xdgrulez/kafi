from kafi.storage_producer import StorageProducer

class KafkaProducer(StorageProducer):
    def __init__(self, kafka_obj, topic, **kwargs):
        super().__init__(kafka_obj, topic, **kwargs)

    #

    def write(self, value, **kwargs):
        return self.produce(value, **kwargs)

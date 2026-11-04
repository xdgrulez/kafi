from kafi.storage_consumer import StorageConsumer

#

class KafkaConsumer(StorageConsumer):
    def __init__(self, kafka_obj, topic, **kwargs):
        super().__init__(kafka_obj, topic, **kwargs)

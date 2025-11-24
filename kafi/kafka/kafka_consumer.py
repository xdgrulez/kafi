from kafi.storage_consumer import StorageConsumer

# Constants

ALL_MESSAGES = -1

#

class KafkaConsumer(StorageConsumer):
    def __init__(self, kafka_obj, *topics, **kwargs):
        super().__init__(kafka_obj, *topics, **kwargs)

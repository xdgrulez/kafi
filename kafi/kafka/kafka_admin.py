from kafi.storage_admin import StorageAdmin

class KafkaAdmin(StorageAdmin):
    def __init__(self, kafka_obj, **kwargs):
        super().__init__(kafka_obj, **kwargs)

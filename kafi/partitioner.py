# Constants

RD_KAFKA_PARTITION_UA = -1

#

def round_robin_partitioner(message_dict, counter_int, partitions_int, key_projection_function=lambda x: x):
    partition_int = message_dict["partition"]
    if partition_int == RD_KAFKA_PARTITION_UA:
        key_bytes = key_projection_function(message_dict["key"])
        #
        if key_bytes is None:
            partition_int = counter_int
            if counter_int == partitions_int - 1:
                counter_int = 0
            else:
                counter_int += 1
        else:
            partition_int = hash(str(key_bytes)) % partitions_int
    #
    return partition_int

# Chunking

def create_chunk_key(key_bytes, chunk_int):
    chunk_key_bytes = key_bytes + bytes(f"_{chunk_int:06}")
    #
    return chunk_key_bytes


def get_chunk_key(chunk_key_bytes):
    key_bytes = chunk_key_bytes[:-7]
    #
    return key_bytes

# Constants

RD_KAFKA_PARTITION_UA = -1

#

def default_partitioner(message_dict, counter_int, partitions_int, projection_function=lambda x: x["key"]):
    partition_int = message_dict["partition"]
    if partition_int == RD_KAFKA_PARTITION_UA:
        bytes = projection_function(message_dict)
        #
        if bytes is None:
            partition_int = counter_int
            if counter_int == partitions_int - 1:
                counter_int = 0
            else:
                counter_int += 1
        else:
            partition_int = hash(str(bytes)) % partitions_int
    #
    return partition_int

# Chunking

def key_to_chunk_key(key_bytes, chunk_int):
    chunk_key_bytes = key_bytes + bytes(f"_{chunk_int:06}")
    #
    return chunk_key_bytes


def chunk_key_to_key(chunk_key_bytes):
    key_bytes = chunk_key_bytes[:-7]
    #
    return key_bytes

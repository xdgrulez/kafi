                        # By convention, committed offsets reflect the next message to be consumed, not the last message consumed. (from https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
                            # storage_id_topic_str_tuple_offsets_dict_dict[storage_id_topic_str_tuple][partition_int] = offset_int + 1

retractions for windows
NamedTuple

# Kafi

## Bugs

* consume with offsets=... only works with a new consumer group

## Schema Registry

* support patterns everywhere

* remove custom REST API calls as far as possible and replace by confluent-kafka-python

* support for Schema GUIDs

## Other

* support all kwargs that make sense globally in config yamls (e.g. the new ones or (de)serializer configs)?

* support for new confluent-kafka-python features (e.g. aio...)

* better error handling (e.g. if topic does not yet exists and auto creation is off, GroupAuthorization, TopicAuthorization...)

* better progress display (produce/consume)

* support for ctrl-c

# Streams

* multiple sinks (=> tn, sink_storage, sink_topic-tuples?)

* spill to disk (slatedb? python-libs?)

* multicore support

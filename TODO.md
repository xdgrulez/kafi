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

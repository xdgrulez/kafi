# consume/foldl/consume_impl

so far:
KafkaConsumer
  consume(self, n=ALL_MESSAGES, **kwargs) -> message_dict_list
    foldl()
      consume_impl(self, **kwargs) -> message_dict_list

FSConsumer
  consume(self, n=ALL_MESSAGES, **kwargs) -> message_dict_list
    foldl()
      (no separate consume_impl)
      
idea:
  separate consume_impl() in FSConsumer
  foldl -> StorageConsumer
  foldl for chunking: collect not only messages but mapping from chunk IDs to chunked messages (or so)

how to dechunk upon consuming:
  * we need state beyond a single poll, e.g. in the consumer object
  * but what state?
  * chunk number
  * number of chunks
  * chunked message; ID = UUID or hash of full message?
  * build up dict like this while consuming: chunked message ID -> chunk number -> message_dict/value
  * if all chunks could be found, add the message to the list of returned messages + delete it in the state 

# other TODOs

* catch ctrl-c for functional not only streams
* aio support...
* support all kwargs that make sense globally in config yamls (e.g. the new ones or (de)serializer configs)?
* do not list internal _topics
* Schema Registry: support patterns
* better error handling (e.g. if topic does not yet exists and auto creation is off, GroupAuthorization, TopicAuthorization...)
* add Perftest?
* local topic consume performance
* debug mode - e.g. show all HTTPS calls (and also confluent_kafka, Local, S3, AzureBlob)
* support schemaregistry-rules (confluent_kafka 2.7.0)
* SR: remove custom API calls for missing ones in ck 2.6.x (and check whether 2.7.0 supports all it should...)

# Methods

## AdminClient

### Topics

* size() (kafka)
* topics()/ls()/l()/ll() (kafka)
* exists() (kafka)

* watermarks()
* list_topics()
* config()
* set_config()
* create()/touch() - (TOOD: support replication factor, support list of topics to be created)
* delete()/rm()
* offsets_for_times()
* X describe()
* partitions()
* set_partitions() (cluster-only)

### Consumer Groups

* groups()
* describe_groups()
* delete_groups() (cluster-only)
* group_offsets()
* alter_group_offsets()

### Brokers

* brokers()
* broker_config()
* set_broker_config()

### ACLs

* acls()
* create_acl()
* delete_acl()

## Producer

* produce() - (TODO: provide schema ID only/no schema)
* flush() (cluster-only)
* purge() (TODO)
* abort_transaction() (TODO)
* begin_transaction() (TODO)
* commit_transaction() (TODO)
* init_transaction() (TODO)
* poll() (TODO)
* send_offsets_to_transaction() (TODO)

better:
* producer() - create producer
* producers() - list producers
* => default: only one producer per e.g. cluster object as in kash.py before

## Consumer

* subscribe() - (TODO: support for multiple topics)
* unsubscribe()
* consume() - (TODO: document consumer_timeout())
* commit()
* offsets()
* close()
* memberid() (cluster-only)
* assign() (TODO?)
* assignment() (TODO?)
* committed() (TODO?)
* incremental_assign() (TODO?)
* incremental_unassign() (TODO?)
* pause() (TODO?)
* position() (TODO?)
* resume() (TODO?)
* seek() (TODO?)
* store_offsets() (TODO?)
* unassign() (TODO?)

cluster:
* subs() (kafka)
* create_subscription_id() (kafka)
* get_subscription_id() (kafka)
* get_subscription() (kafka)

better:
* consumer()
* consumers()
* -> close()
* subscribe()
* subscriptions()/subs()
* -> unsubscribe()
* => default: only one consumer + one subscription per e.g. cluster object as in kash.py before

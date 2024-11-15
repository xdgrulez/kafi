* "repeat last message", produce mit ganzem Message-Record
 * empty topic (evtl. sogar mit Timestamp/Offset)
 * clean up fs groups after offsets.retention.minutes (default: 24h) after last update (explicit call at first?)
* cat mit map_function fixen?
* bessere Fehlermeldung, wenn (lokales) Topic nicht existiert

* Doku:
1. Config
  Kafka (Cluster, RestProxy)
  FS (Local, S3, AzureBlob)
2. High-level
  shell (cat, grep, cp (auch Cluster-übergreifend)...)
  pandas
3. Low-level
  produce
  consume
  map
  flatmap
  foldl...
* Perftest - vielleicht analog zu AK-Distro?
* Nervende Meldungen (Telemetry, Consumer...) weg
* Fehlermeldung GroupAuthorization + TopicAuthorization bei foldl abfangen und anzeigen
* "repeat last message", produce mit ganzem Message-Record
* empty topic (evtl. sogar mit Timestamp/Offset)
* clean up fs groups after offsets.retention.minutes (default: 24h) after last update (explicit call at first?)
* offsets with REST Proxy? Geht es vielleicht doch?
* set default value for key_type/value_type etc. in kash-section
* bring back recreate
* next talk: keywords on slides, no (almost) full sentences, use Jupyter notebook instead of shell

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

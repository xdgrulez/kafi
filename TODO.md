# other TODOs

enable.partition.eof=true
damit bekommt man laut claude error-messages auf jeder partition, die das ende der partition ansagen:
from confluent_kafka import Consumer, KafkaError

consumer = Consumer({
    'bootstrap.servers': '...',
    'group.id': '...',
    'enable.partition.eof': True
})

eof_partitions = set()
assignment = consumer.assignment()

while True:
    msg = consumer.poll(timeout=10.0)  # timeout hoch für Confluent Cloud, kein Problem
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            eof_partitions.add((msg.topic(), msg.partition()))
            if len(eof_partitions) == len(assignment):
                break  # alle Partitionen fertig → sofort raus
    else:
        process(msg)

d.h. wir müssten nicht mehr umsonst consume_timeout lang warten :)

* flatmap:
# zset/functions/linear.py — neue Funktion hinzufügen

from typing import Callable, Iterable

FlatProjection = Callable[[T], Iterable[R]]

def flat_project[T, R](zset: ZSet[T], f: FlatProjection[T, R]) -> ZSet[R]:
    """
    Projects a Z-set to a new Z-set by applying a function that returns
    multiple elements per input. Weights are inherited from the source element.
    """
    output: Dict[R, int] = {}
    for value, weight in zset.items():
        for fvalue in f(value):
            if fvalue not in output:
                output[fvalue] = weight
            else:
                output[fvalue] += weight
    return ZSet(output)

# zset/operators/linear.py — neuer Operator, exakt analog zu LiftedProject

class LiftedFlatProject(Lift1[ZSet[T], ZSet[R]]):
    def __init__(self, stream: Optional[StreamHandle[ZSet[T]]], f: FlatProjection[T, R]):
        super().__init__(stream, lambda z: flat_project(z, f), None)


class LiftedLiftedFlatProject(Lift1[Stream[ZSet[T]], Stream[ZSet[R]]]):
    def __init__(self, stream: Optional[StreamHandle[Stream[ZSet[T]]]], f: FlatProjection[T, R]):
        super().__init__(
            stream, lambda x: step_until_fixpoint_and_return(LiftedFlatProject(StreamHandle(lambda: x), f)), None
        )
* profiling (mem/time), debugging everywhere, clean up and focus on what we have now
* build left join + test
* TopologyNode-Tests -> unittests
* streams-Test -> unittests (share all TopologyNode-Tests with/without kafi-streams on top)

* catch ctrl-c for functional not only streams
* support all kwargs that make sense globally in config yamls (e.g. the new ones or (de)serializer configs)?
* do not list internal _topics
* Schema Registry: support patterns
* better error handling (e.g. if topic does not yet exists and auto creation is off, GroupAuthorization, TopicAuthorization...)
* Kafka-Perftest add-on?
* Kafka emulation performance - indexing?
* debug mode - e.g. show all HTTPS calls (and also confluent_kafka, Local, S3, AzureBlob)
* support schemaregistry-rules (confluent_kafka 2.13.0)
* SR: remove custom API calls for missing ones in ck 2.6.x (and check whether 2.13.0 supports all it should...)
* check what 2.13.0 can do and support it ;)
* aio support?

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

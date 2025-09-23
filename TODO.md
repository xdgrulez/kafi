* catch ctrl-c for functional not only streams
* do not list internal _topics
* "string" as an alias for "str"
* SR: remove custom API calls for missing ones in ck 2.6.x (and check whether 2.7.0+ supports all it should...)

* debug mode - e.g. show all HTTPS calls (and also confluent_kafka, Local, S3, AzureBlob)
* support schemaregistry-rules (confluent_kafka 2.7.0+)
* add Perftest?
* improve emulated Kafka topic performance
* Support for Serializer/Deserializer-Confs 
* aiokafka-Support

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

###

index_function = lambda x: on_function1(x)
l1 = LiftedStreamIntroduction(self._stream_handle)
r1 = LiftedStreamIntroduction(other._stream_handle)
l2 = LiftedLiftedIndex(l1, index_function) # or l1.output_handle()?
r2 = LiftedLiftedIndex(r1, index_function) # or r1.output_handle()?
deltaLiftedDeltaLiftedSortMergeJoin = DeltaLiftedDeltaLiftedSortMergeJoin(
    l2.output_handle(),
    r2.output_handle(),
    projection_function1)

#

-
- new
-

def stream_introduction[T](value: T, group: AbelianGroupOperation[T]) -> Stream[T]:
=> T -> Stream[T]


class LiftedStreamIntroduction(Lift1[T, Stream[T]]):
    def __init__(self, stream: StreamHandle[T]) -> None:
        group = stream.get().group()
        super().__init__(stream, lambda x: stream_introduction(x, group), StreamAddition(group))
=> StreamHandle[T] -> Stream[Stream[T]]


class LiftedLiftedIndex[I, T](Lift1[Stream[ZSet[T]], Stream[IndexedZSet[I, T]]]):
    def __init__(self, stream: Optional[StreamHandle[Stream[ZSet[T]]]], indexer: Indexer[T, I]):
        self.indexer = indexer
        innermost_group: ZSetAddition[T] = ZSetAddition()
        inner_group = IndexedZSetAddition(innermost_group, self.indexer)
        outer_group = StreamAddition(inner_group)
        super().__init__(
            stream,
            lambda sp: step_until_fixpoint_and_return(LiftedIndex(StreamHandle(lambda: sp), indexer)),
            outer_group,
        )
=> StreamHandle[Stream[ZSet[T]]], Indexer[T, I] -> Stream[Stream[IndexedZSet[I, T]]]


class DeltaLiftedDeltaLiftedSortMergeJoin[I, T, R, S](
    BinaryOperator[Stream[IndexedZSet[I, T]], Stream[IndexedZSet[I, R]], Stream[ZSet[S]]]
):
    def __init__(
        self,
        diff_stream_a: Optional[StreamHandle[Stream[IndexedZSet[I, T]]]],
        diff_stream_b: Optional[StreamHandle[Stream[IndexedZSet[I, R]]]],
        f: PostSortMergeJoinProjection[I, T, R, S],
    ):

#

class Lift1(UnaryOperator[T, R]):
    def __init__(
        self,
        stream: Optional[StreamHandle[T]],
        f1: F1[T, R],
        output_stream_group: Optional[AbelianGroupOperation[R]],
    ):
        self.f1 = f1
        self.frontier = 0
        super().__init__(stream, output_stream_group)


class UnaryOperator(Operator[R], Protocol[T, R]):
    def __init__(
        self,
        stream_handle: Optional[StreamHandle[T]],
        output_stream_group: Optional[AbelianGroupOperation[R]],
    ) -> None:
        if stream_handle is not None:
            self.set_input(stream_handle, output_stream_group)


class BinaryOperator(Operator[S], Protocol[T, R, S]):
    def __init__(
        self,
        stream_a: Optional[StreamHandle[T]],
        stream_b: Optional[StreamHandle[R]],
        output_stream_group: Optional[AbelianGroupOperation[S]],
    ) -> None:
        if stream_a is not None:
            self.set_input_a(stream_a)

        if stream_b is not None:
            self.set_input_b(stream_b)

        if output_stream_group is not None:
            output = Stream(output_stream_group)

            self.set_output_stream(StreamHandle(lambda: output))

#

class Operator(Protocol[T]):
    @abstractmethod
    def step(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def output_handle(self) -> StreamHandle[T]:
        raise NotImplementedError

#

class Protocol(Generic, metaclass=_ProtocolMeta):
    """Base class for protocol classes.

    Protocol classes are defined as::

        class Proto(Protocol):
            def meth(self) -> int:
                ...

    Such classes are primarily used with static type checkers that recognize
    structural subtyping (static duck-typing).

    For example::

        class C:
            def meth(self) -> int:
                return 0

        def func(x: Proto) -> int:
            return x.meth()

        func(C())  # Passes static type check

    See PEP 544 for details. Protocol classes decorated with
    @typing.runtime_checkable act as simple-minded runtime protocols that check
    only the presence of given attributes, ignoring their type signatures.
    Protocol classes can be generic, they are defined as::

        class GenProto[T](Protocol):
            def meth(self) -> T:
                ...
    """

-
- old
-

index_function = lambda x: on_function1(x)
left_index = LiftedIndex(self._stream_handle, index_function)
right_index = LiftedIndex(other._stream_handle, index_function)
join_op = Incrementalize2(
    left_index.output_handle(),
    right_index.output_handle(),
    lambda l, r: join_with_index(l, r, projection_function1),
    self.group
)

#

class LiftedIndex[I, T](Lift1[ZSet[T], IndexedZSet[I, T]]):
    def __init__(self, stream: Optional[StreamHandle[ZSet[T]]], indexer: Indexer[T, I]):
        self.indexer = indexer
        inner_group: ZSetAddition[T] = ZSetAddition()
        group = IndexedZSetAddition(inner_group, self.indexer)

        super().__init__(stream, lambda z: index_zset(z, self.indexer), group)
#

class Incrementalize2(BinaryOperator[T, R, S]):
    def __init__(
        self,
        stream_a: Optional[StreamHandle[T]],
        stream_b: Optional[StreamHandle[R]],
        f2: F2[T, R, S],
        output_stream_group: Optional[AbelianGroupOperation[S]],
    ) -> None:
        self.f2 = f2
        super().__init__(stream_a, stream_b, output_stream_group)

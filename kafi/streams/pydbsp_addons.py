from pydbsp.indexed_zset import Indexer, I
from pydbsp.stream import Lift1, Stream, StreamHandle, step_until_fixpoint_and_return
from typing import Callable, Optional, TypeVar

from pydbsp.stream.operators.linear import Differentiate, Integrate, LiftedIntegrate
from pydbsp.zset import ZSetAddition
from pydbsp.zset import ZSet, ZSetAddition

T = TypeVar("T")
R = TypeVar("R")

# We assume that the aggregation function is linear, that is, f(a + b) = f(a) + f(b)
# Mind you, Lift1 and Lift2 functions are for direct manipulation of ZSets, not of their values.
Aggregation = Callable[[ZSet[tuple[I, ZSet[T]]]], ZSet[tuple[I, R]]]

def group_zset(zset: ZSet[T], by: Indexer[T, I]) -> ZSet[tuple[I, ZSet[T]]]:
    grouping: dict[I, ZSet[T]] = {}
    for k, v in zset.items():
        group = by(k)

        if group not in grouping:
            new_zset = ZSet({k: v})
            grouping[group] = new_zset
        else:
            zset_addition: ZSetAddition[T] = ZSetAddition()
            grouping[group] = zset_addition.add(a=grouping[group], b=ZSet({k: v}))

    return ZSet({(group, zset_grouped): 1 for group, zset_grouped in grouping.items()})

class LiftedAggregate(Lift1[ZSet[T], ZSet[tuple[I, R]]]):
    def __init__(self, stream: StreamHandle[ZSet[T]], by: Indexer[T, I], aggregation: Aggregation[I, T, R]):
        super().__init__(stream, lambda zset: aggregation(group_zset(zset, by)), None)

class LiftedLiftedAggregate(Lift1[Stream[ZSet[T]], Stream[ZSet[tuple[I, R]]]]):
    def __init__(self, stream: StreamHandle[Stream[ZSet[T]]], by: Indexer[T, I], aggregation: Aggregation[I, T, R]):
        super().__init__(stream, lambda sp: step_until_fixpoint_and_return(LiftedAggregate(StreamHandle(lambda: sp), by, aggregation)), None)

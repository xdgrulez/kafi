from typing import Callable, Optional

from pydbsp.indexed_zset import Indexer, I

from pydbsp.stream import step_until_fixpoint_and_return, BinaryOperator, Lift1, LiftedGroupAdd, Stream, StreamHandle, UnaryOperator
from pydbsp.stream.operators.linear import Integrate, LiftedIntegrate, LiftedGroupNegate

from pydbsp.zset import ZSet, ZSetAddition
from pydbsp.zset.operators.bilinear import DeltaLiftedDeltaLiftedJoin, JoinCmp, PostJoinProjection, R, S, T
from pydbsp.zset.operators.linear import Cmp, LiftedLiftedProject, LiftedLiftedSelect, Projection as project
from pydbsp.zset.operators.unary import DeltaLiftedDeltaLiftedDistinct

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

def group_by_fst[K, V](input: tuple[K, V]) -> K:
    return input[0]

# This is a proper weighted ZSet sum.
def zset_sum(input: ZSet[tuple[I, ZSet[int]]]) -> ZSet[tuple[I, int]]:
    output_dict: dict[I, int] = {}
    for (group, zset), _ in input.items():
        for k, v in zset.items():
            if group not in output_dict:
                output_dict[group] = k[1] * v
            else:
                output_dict[group] += (k[1] * v)
    
    return ZSet({(group_fst, v): 1 for group_fst, v in output_dict.items()})


class Union(BinaryOperator[Stream[ZSet[T]], Stream[ZSet[T]], Stream[ZSet[T]]]):
    """
    (SELECT * FROM I1)
    UNION
    (SELECT * FROM I2)
    """

    frontier_a: int
    frontier_b: int

    addition: LiftedGroupAdd[Stream[ZSet[T]]]
    distinct: DeltaLiftedDeltaLiftedDistinct[T]

    def set_input_a(self, stream_handle_a: StreamHandle[Stream[ZSet[T]]]) -> None:
        self.input_stream_handle_a = stream_handle_a

    def set_input_b(self, stream_handle_b: StreamHandle[Stream[ZSet[T]]]) -> None:
        self.input_stream_handle_b = stream_handle_b
        self.addition = LiftedGroupAdd(self.input_stream_handle_a, self.input_stream_handle_b)
        self.distinct = DeltaLiftedDeltaLiftedDistinct(self.addition.output_handle())

        self.output_stream = self.distinct.output()
        self.output_stream_handle = self.distinct.output_handle()

    def __init__(self, stream_a: Optional[StreamHandle[Stream[ZSet[T]]]], stream_b: Optional[StreamHandle[Stream[ZSet[T]]]]):
        self.frontier_a = 0
        self.frontier_b = 0

        if stream_a is not None:
            self.set_input_a(stream_a)
        if stream_b is not None:
            self.set_input_b(stream_b)

    def step(self) -> bool:
        current_a_timestamp = self.input_a().current_time()
        current_b_timestamp = self.input_b().current_time()

        if current_a_timestamp == self.frontier_a and current_b_timestamp == self.frontier_b:
            return True

        self.frontier_a += 1
        self.frontier_b += 1

        self.addition.step()

        return self.distinct.step()


class Projection(UnaryOperator[Stream[ZSet[T]], Stream[ZSet[R]]]):
    """
    SELECT DISTINCT I.c
    FROM I
    """
    frontier: int
    stream: StreamHandle[Stream[ZSet[T]]]
    lifted_lifted_project: LiftedLiftedProject[T, R]
    distinct: DeltaLiftedDeltaLiftedDistinct[R]

    # This would require some dependent typing to be properly implemented.
    def __init__(self, stream: StreamHandle[Stream[ZSet[T]]], projection: project[T, R]):
        self.stream = stream
        self.frontier = 0
        self.lifted_lifted_project = LiftedLiftedProject(stream, projection)
        self.distinct = DeltaLiftedDeltaLiftedDistinct(self.lifted_lifted_project.output_handle())
        self.output_stream = self.distinct.output()
        self.output_stream_handle = self.distinct.output_handle()

    def step(self) -> bool:
        latest = self.lifted_lifted_project.input_a().current_time()

        if latest == self.frontier:
            return True

        self.frontier += 1

        self.lifted_lifted_project.step()

        return self.distinct.step()


class Filtering(UnaryOperator[Stream[ZSet[T]], Stream[ZSet[T]]]):
    """
    SELECT DISTINCT * FROM I
    WHERE p(I.c)
    """
    frontier: int
    stream: StreamHandle[Stream[ZSet[T]]]
    lifted_lifted_selection: LiftedLiftedSelect[T]
    distinct: DeltaLiftedDeltaLiftedDistinct[T]

    def __init__(self, stream: StreamHandle[Stream[ZSet[T]]], selection: Cmp[T]):
        self.stream = stream
        self.frontier = 0
        self.lifted_lifted_selection = LiftedLiftedSelect(stream, selection)
        self.distinct = DeltaLiftedDeltaLiftedDistinct(self.lifted_lifted_selection.output_handle())
        self.output_stream = self.distinct.output()
        self.output_stream_handle = self.distinct.output_handle()

    def step(self) -> bool:
        latest = self.lifted_lifted_selection.input_a().current_time()

        if latest == self.frontier:
            return True

        self.frontier += 1

        self.lifted_lifted_selection.step()

        return self.distinct.step()


class Selection(UnaryOperator[Stream[ZSet[T]], Stream[ZSet[R]]]):
    """
    SELECT DISTINCT f(I.c, ...)
    FROM I
    """
    frontier: int
    stream: StreamHandle[Stream[ZSet[T]]]
    lifted_lifted_project: LiftedLiftedProject[T, R]
    distinct: DeltaLiftedDeltaLiftedDistinct[R]

    def __init__(self, stream: StreamHandle[Stream[ZSet[T]]], selection: project[T, R]):
        self.stream = stream
        self.frontier = 0
        self.lifted_lifted_project = LiftedLiftedProject(stream, selection)
        self.distinct = DeltaLiftedDeltaLiftedDistinct(self.lifted_lifted_project.output_handle())
        self.output_stream = self.distinct.output()
        self.output_stream_handle = self.distinct.output_handle()

    
    def step(self) -> bool:
        latest = self.lifted_lifted_project.input_a().current_time()

        if latest == self.frontier:
            return True

        self.frontier += 1

        self.lifted_lifted_project.step()

        return self.distinct.step()


class CartesianProduct(BinaryOperator[Stream[ZSet[T]], Stream[ZSet[R]], Stream[ZSet[tuple[T, R]]]]):
    """
    SELECT I1.*, I2.*
    FROM I1, I2
    """
    frontier_a: int
    frontier_b: int

    cartesian_product: DeltaLiftedDeltaLiftedJoin[T, R, tuple[T, R]]

    def set_input_a(self, stream_handle_a: StreamHandle[Stream[ZSet[T]]]) -> None:
        self.input_stream_handle_a = stream_handle_a

    def set_input_b(self, stream_handle_b: StreamHandle[Stream[ZSet[R]]]) -> None:
        self.input_stream_handle_b = stream_handle_b
        self.cartesian_product = DeltaLiftedDeltaLiftedJoin(self.input_stream_handle_a, self.input_stream_handle_b, lambda x, y: True, lambda x, y: (x, y))
        self.output_stream = self.cartesian_product.output()
        self.output_stream_handle = self.cartesian_product.output_handle()

    def __init__(self, stream_a: Optional[StreamHandle[Stream[ZSet[T]]]], stream_b: Optional[StreamHandle[Stream[ZSet[R]]]]):
        if stream_a is not None:
            self.set_input_a(stream_a)
        if stream_b is not None:
            self.set_input_b(stream_b)
    
    def step(self) -> bool:
        return self.cartesian_product.step()


class Join(BinaryOperator[Stream[ZSet[T]], Stream[ZSet[R]], Stream[ZSet[tuple[T, R]]]]):
    """
    SELECT I1.*, I2.*
    FROM I1 JOIN I2
    ON I1.c1 = I2.c2
    """
    
    join: DeltaLiftedDeltaLiftedJoin[T, R, tuple[T, R]]
    on: JoinCmp[T, R]
    proj: PostJoinProjection[T, R, S]

    def set_input_a(self, stream_handle_a: StreamHandle[Stream[ZSet[T]]]) -> None:
        self.input_stream_handle_a = stream_handle_a

    def set_input_b(self, stream_handle_b: StreamHandle[Stream[ZSet[R]]]) -> None:
        self.input_stream_handle_b = stream_handle_b
        self.join = DeltaLiftedDeltaLiftedJoin(self.input_stream_handle_a, self.input_stream_handle_b, self.on, self.proj)
        self.output_stream = self.join.output()
        self.output_stream_handle = self.join.output_handle()

    def __init__(self, stream_a: Optional[StreamHandle[Stream[ZSet[T]]]], stream_b: Optional[StreamHandle[Stream[ZSet[R]]]], on: JoinCmp[T, R], proj: PostJoinProjection[T, R, S]):
        self.on = on
        self.proj = proj
        if stream_a is not None:
            self.set_input_a(stream_a)
        if stream_b is not None:
            self.set_input_b(stream_b)

    def step(self) -> bool:
        return self.join.step()


class Difference(BinaryOperator[Stream[ZSet[T]], Stream[ZSet[T]], Stream[ZSet[T]]]):
    """
    SELECT * FROM I1
    EXCEPT
    SELECT * FROM I2
    """

    frontier_a: int
    frontier_b: int

    negation: LiftedGroupNegate[Stream[ZSet[T]]]
    addition: LiftedGroupAdd[Stream[ZSet[T]]]
    distinct: DeltaLiftedDeltaLiftedDistinct[T]

    def set_input_a(self, stream_handle_a: StreamHandle[Stream[ZSet[T]]]) -> None:
        self.input_stream_handle_a = stream_handle_a

    def set_input_b(self, stream_handle_b: StreamHandle[Stream[ZSet[T]]]) -> None:
        self.input_stream_handle_b = stream_handle_b

        self.negation = LiftedGroupNegate(self.input_stream_handle_b)
        self.addition = LiftedGroupAdd(self.input_stream_handle_a, self.negation.output_handle())
        self.distinct = DeltaLiftedDeltaLiftedDistinct(self.addition.output_handle())
        self.output_stream = self.distinct.output()
        self.output_stream_handle = self.distinct.output_handle()

    def __init__(self, stream_a: Optional[StreamHandle[Stream[ZSet[T]]]], stream_b: Optional[StreamHandle[Stream[ZSet[T]]]]):
        if stream_a is not None:
            self.set_input_a(stream_a)
        if stream_b is not None:
            self.set_input_b(stream_b)
        
        self.frontier_a = 0
        self.frontier_b = 0
    
    def step(self) -> bool:
        current_a_timestamp = self.input_a().current_time()
        current_b_timestamp = self.input_b().current_time()

        if current_a_timestamp == self.frontier_a and current_b_timestamp == self.frontier_b:
            return True

        self.frontier_a += 1
        self.frontier_b += 1

        self.negation.step()

        self.addition.step()

        return self.distinct.step()


class GroupByThenAgg(UnaryOperator[ZSet[T], ZSet[tuple[I, R]]]):
    """
    SELECT I.c1, AGG(I.c2)
    FROM I
    GROUP BY I.c1
    """

    frontier: int

    integrated_stream: Integrate[Stream[ZSet[T]]]
    lift_integrated_stream: LiftedIntegrate[ZSet[T]]
    lifted_lifted_aggregate: LiftedLiftedAggregate[T, I, R]

    def set_input(self, stream_handle: StreamHandle[Stream[ZSet[T]]]) -> None:
        self.input_stream_handle = stream_handle
        self.integrated_stream = Integrate(stream_handle)
        self.lift_integrated_stream = LiftedIntegrate(self.integrated_stream.output_handle())
        self.lifted_lifted_aggregate = LiftedLiftedAggregate(self.lift_integrated_stream.output_handle(), self.by, self.aggregation)

        self.output_stream = self.lifted_lifted_aggregate.output()
        self.output_stream_handle = self.lifted_lifted_aggregate.output_handle()
    
    def __init__(self, stream: StreamHandle[Stream[ZSet[T]]], by: Indexer[T, I], aggregation: Aggregation[I, T, R]):
        self.by = by
        self.aggregation = aggregation
        self.frontier = 0
        self.set_input(stream)
    
    def step(self) -> bool:
        latest = self.input_stream_handle.get().current_time()

        if latest == self.frontier:
            return True

        self.frontier += 1

        self.integrated_stream.step()
        self.lift_integrated_stream.step()
        self.lifted_lifted_aggregate.step()

        return 

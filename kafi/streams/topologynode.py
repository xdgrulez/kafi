import sys
from types import SimpleNamespace

from pydbsp.progress import Feedback as ProgressFeedback

from pydbsp.circuit import Circuit
from pydbsp.compute import ComputeCtx
from pydbsp.core import Antichain, dbsp_time
from pydbsp.evaluate import Evaluator
from pydbsp.indexed_relational_operators import (
    DeltaLiftedDeltaLiftedGroupBy,
    IndexedDeltaLiftedDeltaLiftedJoin,
    LiftIndex,
    LiftLiftIndex,
)
from pydbsp.indexed_zset import IndexedZSetAddition
from pydbsp.operator import Delay, Differentiate, Input, Integrate, Lift1, Lift2, LiftStreamIntroduction
from pydbsp.relational_operators import (
    DeltaLiftedDeltaLiftedDistinct,
    DeltaLiftedDeltaLiftedJoin,
    LiftProject
)
from pydbsp.storage import DictStorage
from pydbsp.zset import ZSet, ZSetAddition

import copy, uuid

import msgpack

import cloudpickle

#

default_pack_fun = msgpack.packb
default_unpack_fun = lambda x: msgpack.unpackb(x, strict_map_key=False)

#

class TopologyNode:
    def __init__(self, name_str, mother_tn_set, build_fun, **kwargs):
        """Create a topology node based on its set of mother nodes upstream.
        
        Args:
            name_str: name for this node
            mother_tn_set: set of mother nodes upstream
            build_fun: function to wire this node's logic into the pydbsp circuit
            **kwargs: passed through to the underlying node(s)"""
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._mother_tn_set = mother_tn_set
        self._build_fun = build_fun
        #
        self._evaluator = None
        self._output_nodeId = None
        #
        self._pack_fun = kwargs["pack_fun"] if "pack_fun" in kwargs else default_pack_fun
        self._unpack_fun = kwargs["unpack_fun"] if "unpack_fun" in kwargs else default_unpack_fun
        #
        self._to_zSet_fun = kwargs["to_zSet_fun"] if "to_zSet_fun" in kwargs else self.from_records
        self._from_zSet_fun = kwargs["from_zSet_fun"] if "from_zSet_fun" in kwargs else self.to_records
        #
        self._source_str = None
        self._sink_str = None
        self._sink_str_list = None
        #
        self._reset_fun = None

    ###
    # Stateless operators
    ###

    # Map

    def _map(self, _map_fun, **kwargs):
        """Transform each record/weight pair into another record/weight pair..
        
        Args:
            _map_fun: (r, w) -> (r, w) - map function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def __map_fun(zSet):
            out_inner_dict = {}
            for packed_r, w in zSet.inner.items():
                out_r, out_w = _map_fun(tn._unpack_fun(packed_r), w)
                #
                if out_w != 0:
                    out_packed_r = tn._pack_fun(out_r)
                    out_inner_dict[out_packed_r] = out_w
            #
            return ZSet(out_inner_dict)
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = self._output_nodeId
            #
            lift1_nodeId = Lift1(f=__map_fun).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = lift1_nodeId
        #
        current_class = type(self)
        tn = current_class("_map_op", {self}, _build_fun, **kwargs)
        #
        return tn

    def map(self, map_fun, **kwargs):
        """Transform each record into another record.
        
        Args:
            map_fun: r -> r - map function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _map_fun(r, w):
            out_r = map_fun(r)
            #
            return out_r, w
        #
        tn = self._map(_map_fun, **kwargs)
        tn._name_str = "map_op"
        #
        return tn

    def peek(self, prefix_str=None, peek_fun=None, **kwargs):
        """Cause a side-effect on a record. The records pass through unchanged.
        
        Args:
            prefix_str: label printed before each record (if peek_fun is None; default: no prefix)
            peek_fun: r -> None - cause a side-effect on record r (default if peek_fun is None: print)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def map_fun(r):
            peek_fun(r)
            #
            return r
        #
        if peek_fun is None:
            peek_fun = lambda r: print(r) if prefix_str is None else print(f"{prefix_str}: {r}")
        #
        tn = self.map(map_fun, **kwargs)
        tn._name_str = "peek_op"
        #
        return tn

    def _peek(self, prefix_str=None, _peek_fun=None, **kwargs):
        """Cause a side effect on a record/weight pair. The records pass through unchanged.
        
        Args:
            prefix_str: label printed before each record/weight pair (if _peek_fun is None; default: no prefix)
            peek_fun: (r, w) -> None - cause a side-effect on record/weight pair (r, w) (default if _peek_fun is None: print)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _map_fun(r, w):
            _peek_fun(r, w)
            #
            return r, w
        #
        if _peek_fun is None:
            _peek_fun = lambda r, w: print((r, w)) if prefix_str is None else print(f"{prefix_str}: {(r, w)}")
        #
        tn = self._map(_map_fun, **kwargs)
        tn._name_str = "_peek_op"
        #
        return tn

    def _neg(self, **kwargs):
        """Flip the weight of a record.
        
        Args:
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _map_fun(r, w):
            return r, -w
        #
        tn = self._map(_map_fun, **kwargs)
        tn._name_str = "_neg_op"
        #
        return tn

    # Flatmap

    def _flatmap(self, _flatmap_fun, **kwargs):
        """Explode each (r, w) pair into an iterable of (r, w) pairs.
        
        Args:
            _flatmap_fun: (r, w) -> iterable((r, w))
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def __flatmap_fun(zSet):
            out_inner_dict = {}
            for packed_r, w in zSet.inner.items():
                for out_r, out_w in _flatmap_fun(tn._unpack_fun(packed_r), w):
                    out_packed_key_any = tn._pack_fun(out_r)
                    out_inner_dict[out_packed_key_any] = out_inner_dict.get(out_packed_key_any, 0) + out_w
            return ZSet({out_packed_key_any: out_w for out_packed_key_any, out_w in out_inner_dict.items() if out_w != 0})
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = self._output_nodeId
            #
            lift1_nodeId = Lift1(f=__flatmap_fun).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = lift1_nodeId
        #
        current_class = type(self)
        tn = current_class("_flatmap_op", {self}, _build_fun, **kwargs)
        #
        return tn

    def flatmap(self, flatmap_fun, **kwargs):
        """Explode each record into an iterable of records.
        
        Args:
            flatmap_fun: r -> iterable of r
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _flatmap_fun(r, w):
            out_r_set = flatmap_fun(r)
            #
            return [(out_r, w) for out_r in out_r_set]
        #
        tn = self._flatmap(_flatmap_fun, **kwargs)
        tn._name_str = "flatmap_op"
        #
        return tn

    # Filter

    def _filter(self, _filter_fun, **kwargs):
        """Filter record/weight pairs according to a predicate.
        
        Args:
            _filter_fun: internal (r, w) -> bool - filter predicate
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def __filter_fun(zSet):
            out_inner_dict = {}
            for packed_r, w in zSet.inner.items():
                if _filter_fun(tn._unpack_fun(packed_r), w):
                    out_inner_dict[packed_r] = w
            #
            return ZSet(out_inner_dict)
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = self._output_nodeId
            #
            lift1_nodeId = Lift1(f=__filter_fun).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = lift1_nodeId
        #
        current_class = type(self)
        tn = current_class("_filter_op", {self}, _build_fun, **kwargs)
        #
        return tn

    def filter(self, filter_fun, **kwargs):
        """Filter records according to a predicate.
        
        Args:
            filter_fun: r -> bool - filter predicate
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _filter_fun(r, _):
            return filter_fun(r)
        #
        tn = self._filter(_filter_fun, **kwargs)
        tn._name_str = "filter_op"
        #
        return tn

    def merge(self, other_tn, **kwargs):
        """Merge two topology nodes.
        
        Args:
            other_tn: the other topology node
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            lift2_add_nodeId = Lift2(op=g.add).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_liftStreamIntroduction_nodeId))
            #
            tn._output_nodeId = lift2_add_nodeId
        #
        current_class = type(self)
        tn = current_class("merge_op", {self, other_tn}, _build_fun, **kwargs)
        #
        return tn

    ###
    # Stateful operators
    ###

    # Join

    def join_equi(self, right_tn, left_key_fun, right_key_fun, project_fun, **kwargs):
        """Equi-join two topology nodes based on keys from both sides.
        
        Args:
            right_tn: the other topology node to join with
            left_key_fun: l_r -> key - get the key of the left record l_r
            right_key_fun: r_r -> key - get the key of the right record r_r
            project_fun: (l_r, r_r) -> r - projection function for left record l_r and right record r_r
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _left_key_fun(left_packed_r):
            left_r = tn._unpack_fun(left_packed_r)
            return tn._pack_fun(left_key_fun(left_r))
        #
        def _right_key_fun(right_packed_r):
            right_r = tn._unpack_fun(right_packed_r)
            return tn._pack_fun(right_key_fun(right_r))
        #
        def _project_fun(_, left_packed_r, right_packed_r):
            left_r = tn._unpack_fun(left_packed_r)
            right_r = tn._unpack_fun(right_packed_r)
            return tn._pack_fun(project_fun(left_r, right_r))
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            l_g_idx = IndexedZSetAddition(g, _left_key_fun)
            r_g_idx = IndexedZSetAddition(g, _right_key_fun)
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = right_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            l_liftIndex_nodeId = LiftIndex(indexer=_left_key_fun).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId,))
            r_liftIndex_nodeId = LiftIndex(indexer=_right_key_fun).connect(evaluator.circuit, (r_liftStreamIntroduction_nodeId,))
            indexedDeltaLiftedDeltaLiftedJoin_nodeId = IndexedDeltaLiftedDeltaLiftedJoin(
                proj=_project_fun,
                group_a=l_g_idx,
                group_b=r_g_idx,
                out_group=g,
            ).connect(evaluator.circuit, (l_liftIndex_nodeId, r_liftIndex_nodeId))
            #
            tn._output_nodeId = indexedDeltaLiftedDeltaLiftedJoin_nodeId
        #
        current_class = type(self)
        tn = current_class("join_equi_op", {self, right_tn}, _build_fun, **kwargs)
        #
        return tn
    
    def join(self, right_tn, predicate_fun, project_fun, **kwargs):
        """Join two topology nodes based on an arbitrary predicate.
        
        Args:
            right_tn: the other topology node to join with
            predicate_fun: (l_r, r_r) -> bool - the join predicate for left record l_r and right record r_r
            project_fun: (l_r, r_r) -> r - projection function for left record l_r and right record r_r
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _predicate_fun(left_packed_r, right_packed_r):
            left_r = tn._unpack_fun(left_packed_r)
            right_r = tn._unpack_fun(right_packed_r)
            return predicate_fun(left_r, right_r)
        #
        def _project_fun(left_packed_r, right_packed_r):
            left_r = tn._unpack_fun(left_packed_r)
            right_r = tn._unpack_fun(right_packed_r)
            return tn._pack_fun(project_fun(left_r, right_r))
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = right_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            deltaLiftedDeltaLiftedJoin_nodeId = DeltaLiftedDeltaLiftedJoin(
                pred=_predicate_fun,
                proj=_project_fun,
                group_a=g,
                group_b=g,
                out_group=g,
            ).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_liftStreamIntroduction_nodeId))
            #
            tn._output_nodeId = deltaLiftedDeltaLiftedJoin_nodeId
        #
        current_class = type(self)
        tn = current_class("join_op", {self, right_tn}, _build_fun, **kwargs)
        #
        return tn

    # Group By + Aggregation

    def group_by_agg(self, key_fun, value_fun, agg_fun, agg_initial_any, project_fun, **kwargs):
        """Group values by key and aggregate them.
        
        Args:
            key_fun: r -> key_any - function to get the key
            value_fun: r -> value_any - function to get the value to aggregate
            agg_fun: (agg any, value_any) -> aggregate function; agg_any: running aggregate, value_any: current value
            agg_initial_any: initial aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _key_fun(packed_r):
            r = tn._unpack_fun(packed_r)
            return tn._pack_fun(key_fun(r))
        #
        def _value_fun(packed_r):
            r = tn._unpack_fun(packed_r)
            return tn._pack_fun(value_fun(r))
        #
        def _agg_fun(packed_r_w_tuple_list):
            packed_agg_any = _agg_initial
            #
            for packed_r, _ in packed_r_w_tuple_list:
                packed_select_any = _value_fun(packed_r)
                #
                agg_any = tn._unpack_fun(packed_agg_any)
                select_any = tn._unpack_fun(packed_select_any)
                #
                packed_agg_any = tn._pack_fun(agg_fun(agg_any, select_any))
            #
            return packed_agg_any
        #
        def _project_fun(packed_key_any_packed_sum_any_tuple):
            packed_key_any, packed_sum_any = packed_key_any_packed_sum_any_tuple
            r = project_fun(tn._unpack_fun(packed_key_any), tn._unpack_fun(packed_sum_any))
            return tn._pack_fun(r)
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            g_idx = IndexedZSetAddition[str, str](g, _key_fun)
            #
            input_nodeId = self._output_nodeId
            #
            liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, input_nodeId)
            liftLiftIndex_nodeId = LiftLiftIndex(indexer=_key_fun).connect(evaluator.circuit, (liftStreamIntroduction_nodeId,))
            deltaLiftedDeltaLiftedGroupBy_nodeId = DeltaLiftedDeltaLiftedGroupBy(
                aggregate=_agg_fun,
                group=g_idx,
                out_group=g,
            ).connect(evaluator.circuit, (liftLiftIndex_nodeId,))
            liftProject_nodeId = LiftProject(f=_project_fun).connect(evaluator.circuit, (deltaLiftedDeltaLiftedGroupBy_nodeId,))
            integrate_nodeId = Integrate(group=g).connect(evaluator.circuit, (liftProject_nodeId,))
            differentiate_nodeId = Differentiate(group=g).connect(evaluator.circuit, (integrate_nodeId,))
            #
            tn._output_nodeId = differentiate_nodeId
        #
        current_class = type(self)
        tn = current_class("group_by_agg_op", {self}, _build_fun, **kwargs)
        #
        _agg_initial = tn._pack_fun(agg_initial_any)
        #
        return tn

    def group_by_sum(self, key_fun, value_fun, project_fun, sum_initial_any=0, **kwargs):
        """Sum a value per key.
        
        Args:
            key_fun: r -> key_any - function to get the key
            value_fun: r -> value_any - function to get the value to aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            sum_initial_any: initial sum (default: 0)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.group_by_agg(key_fun, value_fun, lambda agg_any, value_any: agg_any + value_any, sum_initial_any, project_fun, **kwargs)
        tn._name_str = "group_by_sum_op"
        #
        return tn

    def group_by_max(self, key_fun, value_fun, project_fun, max_initial_any=0, **kwargs):
        """Maximum of a value per key.
        
        Args:
            key_fun: r -> key_any - function to get the key
            value_fun: r -> value_any - function to get the value to aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            max_initial_any: initial maximum (default: 0)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.group_by_agg(key_fun, value_fun, lambda agg_any, value_any: max(agg_any, value_any), max_initial_any, project_fun, **kwargs)
        tn._name_str = "group_by_max_op"
        #
        return tn

    def group_by_min(self, key_fun, value_fun, project_fun, min_initial_any=sys.maxsize, **kwargs):
        """Minimum of a value per key.
        
        Args:
            key_fun: r -> key_any - function to get the key
            value_fun: r -> value_any - function to get the value to aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            min_initial_any: initial minimum (default: sys.maxsize)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.group_by_agg(key_fun, value_fun, lambda agg_any, value_any: min(agg_any, value_any), min_initial_any, project_fun, **kwargs)
        tn._name_str = "group_by_min_op"
        #
        return tn

    def group_by_avg(self, key_fun, value_fun, project_fun, **kwargs):
        """Average of a value per key.
        
        Args:
            key_fun: r -> key_any - function to get the key
            value_fun: r -> value_any - function to get the value to aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _agg_fun(agg_tuple, any):
            sum_any, count_int = agg_tuple
            return (sum_any + any, count_int + 1)
        #
        def _project_fun(key_any, sum_count_tuple):
            sum_any, count_int = sum_count_tuple
            avg_any = sum_any / count_int if count_int != 0 else 0.0
            return project_fun(key_any, avg_any)
        #
        tn = self.group_by_agg(key_fun, value_fun, _agg_fun, (0, 0), _project_fun, **kwargs)
        tn._name_str = "group_by_avg_op"
        #
        return tn

    def group_by_count(self, key_fun, project_fun, **kwargs):
        """Count records per key.
        
        Args:
            key_fun: r -> key_any - function to get the key
            project_fun: (key_any, agg_any) -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.group_by_sum(key_fun, lambda _: 1, project_fun, **kwargs)
        tn._name_str = "group_by_count_op"
        #
        return tn

    # Aggregation

    def agg(self, value_fun, agg_fun, agg_initial_any, project_fun, **kwargs):
        """Aggregate a value.
        
        Args:
            value_fun: r -> value_any - function to get the value to aggregate
            agg_fun: (agg any, value_any) -> aggregate function; agg_any: running aggregate, value_any: current value
            agg_initial_any: initial aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.group_by_agg(lambda _: 0, value_fun, agg_fun, agg_initial_any, lambda _, value_any: project_fun(value_any), **kwargs)
        tn._name_str = "agg_op"
        #
        return tn

    def sum(self, value_fun, project_fun=lambda agg_any: agg_any, sum_initial_any=0, **kwargs):
        """Sum a value.
        
        Args:
            value_fun: r -> value_any - function to get the value to aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            sum_initial_any: initial sum (default: 0)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.agg(value_fun, lambda agg_any, value_any: agg_any + value_any, sum_initial_any, project_fun, **kwargs)
        tn._name_str = "sum_op"
        #
        return tn

    def max(self, value_fun, project_fun=lambda agg_any: agg_any, max_initial_any=0, **kwargs):
        """Maximum of a value.
        
        Args:
            value_fun: r -> value_any - function to get the value to aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            max_initial_any: initial maximum (default: 0)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.agg(value_fun, lambda agg_any, value_any: max(agg_any, value_any), max_initial_any, project_fun, **kwargs)
        tn._name_str = "max_op"
        #
        return tn

    def min(self, value_fun, project_fun=lambda agg_any: agg_any, min_initial_any=sys.maxsize, **kwargs):
        """Minimum of a value.
        
        Args:
            value_fun: r -> value_any - function to get the value to aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            min_initial_any: initial minimum (default: sys.maxsize)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.agg(value_fun, lambda agg_any, value_any: min(agg_any, value_any), min_initial_any, project_fun, **kwargs)
        tn._name_str = "min_op"
        #
        return tn
    
    def avg(self, value_fun, project_fun=lambda agg_any: agg_any, **kwargs):
        """Average of a value.
        
        Args:
            value_fun: r -> value_any - function to get the value to aggregate
            project_fun: (key_any, agg_any) -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.group_by_avg(lambda _: 0, value_fun, lambda _, agg_any: project_fun(agg_any), **kwargs)
        tn._name_str = "avg_op"
        #
        return tn

    def count(self, project_fun=lambda agg_any: agg_any, **kwargs):
        """Count records.
        
        Args:
            project_fun: (key_any, agg_any) -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.sum(lambda _: 1, project_fun, **kwargs)
        tn._name_str = "count_op"
        #
        return tn

    # Distinct

    def distinct(self, **kwargs):
        """Deduplicate.
        
        Args:
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, input_nodeId)
            deltaLiftedDeltaLiftedDistinct_nodeId = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (liftStreamIntroduction_nodeId,))
            #
            tn._output_nodeId = deltaLiftedDeltaLiftedDistinct_nodeId
        #
        current_class = type(self)
        tn = current_class("distinct_op", {self}, _build_fun, **kwargs)
        #
        return tn

    # Union

    def union(self, other_tn, **kwargs):
        """Set union of two topology nodes.
         
        Args:
            other_tn: the other topology node to combine with
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            lift2_add_nodeId = Lift2(op=g.add).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_liftStreamIntroduction_nodeId))
            deltaLiftedDeltaLiftedDistinct_nodeId = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (lift2_add_nodeId,))
            integrate_nodeId = Integrate(group=g).connect(evaluator.circuit, (deltaLiftedDeltaLiftedDistinct_nodeId,))
            #
            tn._output_nodeId = integrate_nodeId
        #
        current_class = type(self)
        tn = current_class("union_op", {self, other_tn}, _build_fun, **kwargs)
        #
        return tn

    # Intersect

    def intersect(self, other_tn, **kwargs):
        """Set intersection of two topology nodes.
        
        Args:
            other_tn: the other topology node to combine with
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = self.join(other_tn, lambda l, r: l == r, lambda l, _: l, **kwargs)
        tn._name_str = "intersect_op"
        #
        return tn

    # Minus

    def minus(self, right_tn, **kwargs):
        """Set difference: self minus right_tn.
        
        Args:
            right_tn: the other topology node to subtract
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = right_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            r_lift1_neg_nodeId = Lift1(f=g.neg).connect(evaluator.circuit, (r_liftStreamIntroduction_nodeId,))
            lift2_add_nodeId = Lift2(op=g.add).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_lift1_neg_nodeId))
            deltaLiftedDeltaLiftedDistinct_nodeId = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (lift2_add_nodeId,))
            #
            tn._output_nodeId = deltaLiftedDeltaLiftedDistinct_nodeId
        #
        current_class = type(self)
        tn = current_class("minus_op", {self, right_tn}, _build_fun, **kwargs)
        #
        return tn

    ###
    # Expiry
    ###

    def expire(self, ts_fun, expiry_fun, project_fun=lambda r_end_ts_tuple: r_end_ts_tuple[0], **kwargs):
        """Expire records once the global watermark passes their expiry timestamp.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            expiry_fun: ts -> expiry ts - get expiry function
            project_fun: (r, end_ts) -> r - projection function (default: lambda r_end_ts_tuple: r_end_ts_tuple[0] i.e., drop the window end timestamp)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        input_plus_expiry_tn = (
            self
            .map(lambda r: (r, expiry_fun(ts_fun(r))), **kwargs)
        )
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #   
            NEG_INF = float("-inf")
            g = ZSetAddition()
            input_nodeId = input_plus_expiry_tn._output_nodeId
            #
            def _ts_fun(packed_r):
                r, _ = tn._unpack_fun(packed_r)
                return ts_fun(r)
            #
            def _expiry_fun(packed_r):
                _, expiry_int = tn._unpack_fun(packed_r)
                return expiry_int
            #
            def _compute(t, reads, ctx):
                read_input, read_self = reads
                #                
                chain = ctx.lattice.factors[0]
                predecessor = chain.predecessor(t[0])
                #
                if predecessor is None:
                    prev_zSet, prev_max_ts = g.identity(), NEG_INF
                else:
                    pred_t = (predecessor,) + t[1:]
                    prev_zSet, prev_max_ts, _ = read_self(pred_t)
                #
                input_zSet = read_input(t)
                #
                input_max_ts = max(
                    (_ts_fun(k) for k, w in input_zSet.items() if w > 0),
                    default=None,
                )
                max_ts = prev_max_ts if input_max_ts is None else max(prev_max_ts, input_max_ts)
                #
                merged_zSet = g.add(prev_zSet, input_zSet)
                #                
                new_state_dict = {}
                expired_dict = {}
                #
                for k, w in merged_zSet.items():
                    if w == 0:
                        continue
                    if _expiry_fun(k) >= max_ts:
                        new_state_dict[k] = w
                    else:
                        expired_dict[k] = -w
                #                
                new_state_zSet = ZSet(new_state_dict)
                expired_zSet = ZSet(expired_dict)
                #
                delta_zSet = g.add(input_zSet, expired_zSet)
                #
                return new_state_zSet, max_ts, delta_zSet
            #
            next_nodeId = evaluator.circuit.next_id()
            expire_nodeId = evaluator.circuit.add(
                ProgressFeedback(input=input_nodeId, self_id=next_nodeId, axis=0),
                SimpleNamespace(compute=_compute),
            )
            #
            project_nodeId = Lift1(f=lambda tup: tup[2]).connect(
                evaluator.circuit, (expire_nodeId,))
            #            
            tn._output_nodeId = project_nodeId
        #
        current_class = type(self)
        tn = current_class("expire_op", {input_plus_expiry_tn}, _build_fun, **kwargs)
        #
        return tn.map(project_fun, **kwargs)

    ###
    # Time Windows - Assign end of window(s) for a timestamp
    ###

    @staticmethod
    def _assign_tumbling(size_int):
        """Get window end assignment function for tumbling windows of a given size.
        
        Args:
            size_int: window size
        Returns:
            ts -> list(end_ts) - function mapping timestamps to lists of window end timestamps"""
        def _assign_fun(ts):
            end_ts_list = [(ts // size_int) * size_int + size_int]
            #
            return end_ts_list
        #
        return _assign_fun

    @staticmethod
    def _assign_hopping(size_int, hop_int):
        """Get window end assignment function for hopping windows of a given size and hop size.
        
        Args:
            size_int: window size
            hop_int: hop size between windows; hop_int <= size_int
        Returns:
            ts -> list(end_ts) - function mapping timestamps to lists of window end timestamps"""
        def _assign_fun(ts):
            first_end_ts = (ts // hop_int) * hop_int + hop_int
            #
            end_ts_list = [first_end_ts + i * hop_int for i in range(size_int // hop_int) if first_end_ts + i * hop_int >= size_int]
            #
            return end_ts_list
        #
        return _assign_fun

    @staticmethod
    def _assign_cumulative(size_int, step_int):
        """Get window end assignment function for cumulative windows of a given size and step size.
        
        Args:
            size_int: window size
            step_int: step size between windows
        Returns:
            ts -> list(end_ts) - function mapping timestamps to lists of window end timestamps"""
        def _assign_fun(ts):
            cumulative_start_ts = (ts // size_int) * size_int
            cumulative_end_ts = cumulative_start_ts + size_int
            #
            first_step_end_ts = (ts // step_int) * step_int + step_int
            end_ts_list = [step_end_ts
                          for step_end_ts in range(first_step_end_ts, cumulative_end_ts + step_int, step_int)
                          if step_end_ts <= cumulative_end_ts]
            #
            return end_ts_list
        #
        return _assign_fun

    @staticmethod
    def _assign_sliding(size_int):
        """Get window end assignment function for sliding windows of a given size.
         
        Args:
            size_int: window size
        Returns:
            ts -> list(end_ts) - function mapping timestamps to lists of window end timestamps"""
        def _assign_fun(ts):
            end_ts_list = [ts + size_int]
            #
            return end_ts_list
        #
        return _assign_fun

    @staticmethod
    def _assign_session(max_session_int):
        """Get window end assignment function for session windows of a given maximum session size.
        
        Args:
            max_session_int: maximum session size
        Returns:
            ts -> list(end_ts) - function mapping timestamps to lists of window end timestamps"""
        def _assign_fun(ts):
            end_ts_list = [(ts // max_session_int) * max_session_int + max_session_int]
            #
            return end_ts_list
        #
        return _assign_fun

    ###
    # Timw Windows - Expiry
    ###

    def expire_tumbling(self, ts_fun, size_int, allowed_lateness_int=0, **kwargs):
        """Expire records once past their tumbling window.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            allowed_lateness_int: allowed lateness
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        _assign_fun = TopologyNode._assign_tumbling(size_int)
        #
        buffer_int = size_int
        #
        tn = self.expire(ts_fun,
                        lambda ts: max(_assign_fun(ts)) + buffer_int + allowed_lateness_int,
                         **kwargs)
        #
        return tn

    def expire_hopping(self, ts_fun, size_int, hop_int, allowed_lateness_int=0, **kwargs):
        """Expire records once past their latest hopping window.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            hop_int: hop between windows, hop_int <= size_int
            allowed_lateness_int: allowed lateness
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        _assign_fun = TopologyNode._assign_hopping(size_int, hop_int)
        #
        buffer_int = size_int
        #
        tn = self.expire(ts_fun,
                         lambda ts: max(_assign_fun(ts)) + buffer_int + allowed_lateness_int,
                         **kwargs)
        #
        return tn

    def expire_cumulative(self, ts_fun, size_int, step_int, allowed_lateness_int=0, **kwargs):
        """Expire records once past their cumulative window.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            step_int: step size between windows
            allowed_lateness_int: allowed lateness
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        _assign_fun = TopologyNode._assign_cumulative(size_int, step_int)
        #
        buffer_int = size_int
        #
        tn = self.expire(ts_fun,
                         lambda ts: max(_assign_fun(ts)) + buffer_int + allowed_lateness_int,
                         **kwargs)
        #
        return tn

    
    def expire_sliding(self, ts_fun, size_int, allowed_lateness_int, **kwargs):
        """Expire records once past their own sliding window.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            allowed_lateness_int: allowed lateness
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        _assign_fun = TopologyNode._assign_sliding(size_int)
        #
        tn = self.expire(ts_fun,
                         lambda ts: max(_assign_fun(ts)) + allowed_lateness_int,
                         **kwargs)
        #
        return tn

    def expire_session(self, ts_fun, max_session_int, allowed_lateness_int=0, **kwargs):
        """Expire records once past their session boundary.
         
        Args:
            ts_fun: r -> ts - get timestamp function
            max_session_int: grid size used internally while bucketing session windows
            allowed_lateness_int: allowed lateness
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        _assign_fun = TopologyNode._assign_session(max_session_int)
        #
        tn = self.expire(ts_fun,
                         lambda ts: max(_assign_fun(ts)) + allowed_lateness_int,
                         **kwargs)
        #
        return tn

    ###
    # Time Windows - Group By + Agg
    ###

    def __group_by_agg_aligned(self, ts_fun, assign_fun, key_fun, agg_fun, agg_initial_any, project_fun, **kwargs):
        """Shared grid-window aggregation: assign to window(s), group, fold.

        Args:
            ts_fun: r -> ts - get timestamp function
            assign_fun: ts -> list(end_ts) - assign window end timestamps
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg any, r_any) -> agg_any - aggregate function
            agg_initial_any: initial aggregate
            project_fun: agg_r -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        _project_fun = lambda by_r_end_ts_tuple, agg_r: (project_fun(by_r_end_ts_tuple[0], agg_r), by_r_end_ts_tuple[1])
        #
        tn = (
            self
            .flatmap(lambda r: [(r, end_ts) for end_ts in assign_fun(ts_fun(r))], **kwargs)
            .group_by_agg(
                lambda r_end_ts_tuple: (key_fun(r_end_ts_tuple[0]), r_end_ts_tuple[1]),
                lambda r_end_ts_tuple: r_end_ts_tuple[0],
                agg_fun,
                agg_initial_any,
                _project_fun,
                **kwargs)
        )
        #
        return tn
    
    def _group_by_agg_tumbling(self, ts_fun, size_int, key_fun, agg_fun, agg_initial_any, project_fun, **kwargs):
        """Aggregate per key over tumbling windows.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg any, r_any) -> agg_any - aggregate function
            agg_initial_any: initial aggregate
            project_fun: agg_r -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = (
            self
            .__group_by_agg_aligned(ts_fun,
                                    TopologyNode._assign_tumbling(size_int),
                                    key_fun,
                                    agg_fun,
                                    agg_initial_any,
                                    project_fun,
                                    **kwargs)
        )
        #
        return tn

    def _group_by_agg_hopping(self, ts_fun, size_int, hop_int, key_fun, agg_fun, agg_initial_any, project_fun, **kwargs):
        """Aggregate per key over hopping windows.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            hop_int: hop size between windows, hop_int <= size_int
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg any, r_any) -> agg_any - aggregate function
            agg_initial_any: initial aggregate
            project_fun: agg_r -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = (
            self
            .__group_by_agg_aligned(ts_fun,
                                    TopologyNode._assign_hopping(size_int, hop_int),
                                    key_fun,
                                    agg_fun,
                                    agg_initial_any,
                                    project_fun,
                                    **kwargs)
        )
        #
        return tn

    def _group_by_agg_cumulative(self, ts_fun, size_int, step_int, key_fun, agg_fun, agg_initial_any, project_fun, **kwargs):
        """Aggregate per key over cumulative windows.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            step_int: step size between windows
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg any, r_any) -> agg_any - aggregate function
            agg_initial_any: initial aggregate
            project_fun: agg_r -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        tn = (
            self
            .__group_by_agg_aligned(ts_fun,
                                    TopologyNode._assign_cumulative(size_int, step_int),
                                    key_fun,
                                    agg_fun,
                                    agg_initial_any,
                                    project_fun,
                                    **kwargs)
        )
        #
        return tn

    #

    def _group_by_agg_sliding(self, ts_fun, size_int, key_fun, agg_fun, agg_initial_any, project_fun, **kwargs):
        """Aggregate per key over overlapping, per-r sliding windows.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg any, r_any) -> agg_any - aggregate function
            agg_initial_any: initial aggregate
            project_fun: agg_r -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def insert_record(agg_r, r):
            ts = ts_fun(r)
            r_list = agg_r["candidates"]
            windows_dict = agg_r["windows"]
            #
            r_list.append(r)
            #
            for start_ts, window_dict in windows_dict.items():
                if start_ts <= ts < start_ts + size_int:
                    window_dict["members"].append(r)
                    window_dict["agg"] = agg_fun(window_dict["agg"], r)
            #
            if ts not in windows_dict:
                member_r_list = [r for r in r_list if ts <= ts_fun(r) < ts + size_int]
                agg_any = agg_initial_any
                for member_r in member_r_list:
                    agg_any = agg_fun(agg_any, member_r)
                windows_dict[ts] = {"members": member_r_list, "agg": agg_any}
            #
            return {"candidates": r_list, "windows": windows_dict}
        #
        def _flatmap_fun(key_any_agg_r_tuple):
            key_any, agg_r = key_any_agg_r_tuple
            return [(project_fun(key_any, wd["agg"]), int(start_str) + size_int) for start_str, wd in agg_r["windows"].items()]
        #
        tn = (
            self
            .group_by_agg(
                key_fun,
                lambda r: r,
                insert_record,
                {"candidates": [], "windows": {}},
                lambda key_any, agg_r: (key_any, agg_r),
                **kwargs
            )
            .flatmap(_flatmap_fun, **kwargs)
        )
        return tn

    #

    def _group_by_agg_session(self, ts_fun, gap_int, key_fun, agg_fun, agg_initial_any, project_fun, **kwargs):
        """Aggregate per key over merging session windows.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            gap_int: gap size
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg any, r_any) -> agg_any - aggregate function
            agg_initial_any: initial aggregate
            project_fun: agg_r -> r - projection function
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        def insert_session(r, session_dict_list):
            ts = ts_fun(r)
            #
            left_session_dict = next((session_dict 
                                      for session_dict in session_dict_list 
                                      if session_dict["start"] - gap_int <= ts <= session_dict["last_ts"] + gap_int), None)
            #
            if left_session_dict:
                left_session_dict["records"].append(r)
                left_session_dict["start"] = min(left_session_dict["start"], ts)
                left_session_dict["last_ts"] = max(left_session_dict["last_ts"], ts)
                left_session_dict["agg"] = agg_fun(left_session_dict["agg"], r)
                #
                right_session_dict = next((session_dict 
                                           for session_dict in session_dict_list 
                                           if session_dict != left_session_dict 
                                           and session_dict["start"] - gap_int <= left_session_dict["last_ts"] + gap_int 
                                           and left_session_dict["start"] - gap_int <= session_dict["last_ts"]), None)
                if right_session_dict:
                    left_session_dict["records"].extend(right_session_dict["records"])
                    left_session_dict["start"] = min(left_session_dict["start"], right_session_dict["start"])
                    left_session_dict["last_ts"] = max(left_session_dict["last_ts"], right_session_dict["last_ts"])
                    #
                    left_session_dict["records"].sort(key=ts_fun)
                    #                    
                    agg_any = agg_initial_any.copy()
                    for r in left_session_dict["records"]:
                        agg_any = agg_fun(agg_any, r)
                    left_session_dict["agg"] = agg_any
                    #
                    session_dict_list.remove(right_session_dict)
            else:
                session_dict_list.append({
                    "start": ts,
                    "last_ts": ts,
                    "records": [r],
                    "agg": agg_fun(agg_initial_any.copy(), r)
                })
            #
            session_dict_list.sort(key=lambda session_dict: session_dict["start"])
            #
            return session_dict_list
        #
        def _flatmap_fun(key_any_agg_any_session_end_ts_tuple_list):
            return [(project_fun(key_any_agg_any_session_end_ts_tuple_list[0], agg_any_session_end_ts_tuple[0]), agg_any_session_end_ts_tuple[1])
                    for agg_any_session_end_ts_tuple in key_any_agg_any_session_end_ts_tuple_list[1]]
        #
        tn = (
            self
            .group_by_agg(
                key_fun,
                lambda r: r,
                lambda agg_r, r:
                {"sessions": (session_dict_list := insert_session(r, agg_r.get("sessions", []))),
                 "output": [(session_dict["agg"], session_dict["last_ts"] + gap_int) for session_dict in session_dict_list]},
                {"sessions": [], "output": []},
                lambda by, agg_r: (by, agg_r["output"]),
                **kwargs
            )
            .flatmap(_flatmap_fun, **kwargs)
        )
        return tn

    ###
    # Time Windows - Trigger
    ###

    def _trigger(self, time_tn, ts_fun, trigger_fun=lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1], project_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]}, positive_only_bool=True, **kwargs):
        """Emit a window's result once the watermark passes its end.
        
        Args:
            time_tn: node whose ts drive the watermark used for triggering
            ts_fun: r -> ts function
            trigger_fun: ((r, end ts), latest ts) -> bool, predicate to trigger the emission of a window (default: lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1])
            project_fun: (r, end_ts) -> projection function (default: lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
            positive_only_bool: if True, suppress retractions (w <= 0) from the output (default: True)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        trigger_tn = (
            self
            .join(time_tn.max(ts_fun),
                  lambda r_end_ts_tuple, latest_ts: trigger_fun(r_end_ts_tuple, latest_ts),
                  lambda r_end_ts_tuple, _: project_fun(r_end_ts_tuple),
                  **kwargs)
        )
        trigger_tn = trigger_tn._filter(lambda _, w: w > 0, **kwargs) if positive_only_bool else trigger_tn
        #
        return trigger_tn

    ###
    # Time Windows - {Group By, Trigger}
    ###

    def group_by_agg_tumbling(self, ts_fun, size_int, key_fun, agg_fun, agg_initial_any, project_fun, trigger_fun=lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1], trigger_project_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]}, trigger_positive_only_bool=True, **kwargs):
        """Tumbling window aggregation, emitted once each window closes or using a custom trigger function.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg_r, r) -> agg_r - aggregate function
            agg_initial_any: initial aggregate
            project_fun: key_any, agg_r -> r - projection function
            trigger_fun: ((r, end ts), latest ts) -> bool, predicate to trigger the emission of a window (default: lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1])
            project_fun: (r, end_ts) -> projection function (default: lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
            positive_only_bool: if True, suppress retractions (w <= 0) from the output (default: True)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        group_by_agg_tn = (
            self
            ._group_by_agg_tumbling(ts_fun,
                                    size_int,
                                    key_fun,
                                    agg_fun,
                                    agg_initial_any,
                                    project_fun,
                                    **kwargs)
        )
        #
        trigger_tn = group_by_agg_tn._trigger(self,
                                              ts_fun,
                                              trigger_fun,
                                              trigger_project_fun,
                                              trigger_positive_only_bool,
                                              **kwargs)
        #
        return trigger_tn

    #

    def group_by_agg_hopping(self, ts_fun, size_int, hop_int, key_fun, agg_fun, agg_initial_any, project_fun, trigger_fun=lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1], trigger_project_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]}, trigger_positive_only_bool=True, **kwargs):
        """Hopping window aggregation, emitted once each window closes or using a custom trigger function.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            hop_int: hop size
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg_r, r) -> agg_r - aggregate function
            agg_initial_any: initial aggregate
            project_fun: key_any, agg_r -> r - projection function
            trigger_fun: ((r, end ts), latest ts) -> bool, predicate to trigger the emission of a window (default: lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1])
            project_fun: (r, end_ts) -> projection function (default: lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
            positive_only_bool: if True, suppress retractions (w <= 0) from the output (default: True)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        group_by_agg_tn = (
            self
            ._group_by_agg_hopping(ts_fun,
                                   size_int,
                                   hop_int,
                                   key_fun,
                                   agg_fun,
                                   agg_initial_any,
                                   project_fun,
                                   **kwargs)
        )
        #
        trigger_tn = group_by_agg_tn._trigger(self,
                                              ts_fun,
                                              trigger_fun,
                                              trigger_project_fun,
                                              trigger_positive_only_bool,
                                              **kwargs)
        #
        return trigger_tn

    #

    def group_by_agg_cumulative(self, ts_fun, size_int, step_int, key_fun, agg_fun, agg_initial_any, project_fun, trigger_fun=lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1], trigger_project_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]}, trigger_positive_only_bool=True, **kwargs):
        """Cumulative window aggregation, emitted once each window closes or using a custom trigger function.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            step_int: step size
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg_r, r) -> agg_r - aggregate function
            agg_initial_any: initial aggregate
            project_fun: key_any, agg_r -> r - projection function
            trigger_fun: ((r, end ts), latest ts) -> bool, predicate to trigger the emission of a window (default: lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1])
            project_fun: (r, end_ts) -> projection function (default: lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
            positive_only_bool: if True, suppress retractions (w <= 0) from the output (default: True)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        group_by_agg_tn = (
            self
            ._group_by_agg_cumulative(ts_fun,
                                      size_int,
                                      step_int,
                                      key_fun,
                                      agg_fun,
                                      agg_initial_any,
                                      project_fun,
                                      **kwargs)
        )
        #
        trigger_tn = group_by_agg_tn._trigger(self,
                                              ts_fun,
                                              trigger_fun,
                                              trigger_project_fun,
                                              trigger_positive_only_bool,
                                              **kwargs)
        #
        return trigger_tn

    #

    def group_by_agg_sliding(self, ts_fun, size_int, key_fun, agg_fun, agg_initial_any, project_fun, trigger_project_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]}, trigger_positive_only_bool=True, **kwargs):
        """Sliding window aggregation; emitted once each window closes or using a custom trigger function.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg_r, r) -> agg_r - aggregate function
            agg_initial_any: initial aggregate
            project_fun: key_any, agg_r -> r - projection function
            trigger_fun: ((r, end ts), latest ts) -> bool, predicate to trigger the emission of a window (default: lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1])
            project_fun: (r, end_ts) -> projection function (default: lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
            positive_only_bool: if True, suppress retractions (w <= 0) from the output (default: True)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        group_by_agg_tn = self._group_by_agg_sliding(ts_fun,
                                                     size_int,
                                                     key_fun,
                                                     agg_fun,
                                                     agg_initial_any,
                                                     project_fun,
                                                     **kwargs)
        trigger_tn = group_by_agg_tn.map(trigger_project_fun)
        #
        trigger_tn = trigger_tn._filter(lambda _, w: w > 0) if trigger_positive_only_bool else trigger_tn
        #
        return trigger_tn
    
    #

    def group_by_agg_session(self, ts_fun, gap_int, key_fun, agg_fun, agg_initial_any, project_fun, trigger_fun=lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1], trigger_project_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]}, trigger_positive_only_bool=True, **kwargs):
        """Session window aggregation, emitted once each session closes or using a custom trigger function.
        
        Args:
            ts_fun: r -> ts - get timestamp function
            size_int: window size
            gap_int: gap size
            key_fun: r -> key_any - grouping key function
            agg_fun: (agg_r, r) -> agg_r - aggregate function
            agg_initial_any: initial aggregate
            project_fun: key_any, agg_r -> r - projection function
            trigger_fun: ((r, end ts), latest ts) -> bool, predicate to trigger the emission of a window (default: lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1])
            project_fun: (r, end_ts) -> projection function (default: lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
            positive_only_bool: if True, suppress retractions (w <= 0) from the output (default: True)
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created topology node of the operator"""
        group_by_agg_tn = (
            self
            ._group_by_agg_session(ts_fun,
                                   gap_int,
                                   key_fun, 
                                   agg_fun,
                                   agg_initial_any,
                                   project_fun,
                                   **kwargs)
        )
        #
        trigger_tn = group_by_agg_tn._trigger(self,
                                             ts_fun,
                                             trigger_fun,
                                             trigger_project_fun,
                                             trigger_positive_only_bool,
                                             **kwargs)
        #
        return trigger_tn

    ###
    # Operator utils
    ###

    @staticmethod
    def liftStreamIntroduction(g, evaluator, nodeId):
        """Lift a pydbsp node if necessary.
        
        Args:
            g: ZSetAddition group used to combine Z-sets
            evaluator: pydbsp Evaluator running the circuit
            nodeId: node ID to lift into the circuit
        Returns:
            nodeId: the same pydbsp node (if already lifted) or the lifted pydbsp node"""
        return nodeId if evaluator.frontiers()[nodeId].lattice.nestedness == 2 else LiftStreamIntroduction(group=g).connect(evaluator.circuit, (nodeId,))

    ###
    # Sources and sinks
    ###

    @staticmethod
    def source(source_str, **kwargs):
        """Create a named input source node.
        
        Args:
            source_str: name of the input source
            **kwargs: passed through to the underlying node(s)
        Returns:
            tn: the newly created source topology node"""
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = Input(frontier=Antichain(dbsp_time(1))).connect(evaluator.circuit, ())
            #
            tn._output_nodeId = input_nodeId
        #
        tn = TopologyNode(f"source_{source_str}", {}, _build_fun, **kwargs)
        tn._source_str = source_str
        #
        return tn

    def sink(self, sink_str):
        """Mark this node as a named output sink.
        
        Args:
            sink_str: name of the output sink
        Returns:
            tn: the marked topologynode"""
        self._sink_str = sink_str
        #
        return self

    #

    @staticmethod
    def _merge_sinks(*sink_tn_tuple):
        """Merge sinks.
        
        Args:
            *sink_tn_tuple: one or more sink tn to merge
        Returns:
            built_tn: the built topology node after the sink merge step"""
        sink_str_sink_tn_tuple_list = [(sink_tn._sink_str, sink_tn) for sink_tn in sink_tn_tuple if sink_tn._sink_str is not None]
        if sink_str_sink_tn_tuple_list == []:
            if len(sink_tn_tuple) == 1:
                return sink_tn_tuple[0]
            else:
                raise Exception("Cannot build multiple non-sink nodes.")
        #
        head_sink_str_sink_tn_tuple, *tail_sink_str_sink_tn_tuple_list = sink_str_sink_tn_tuple_list
        #
        head_sink_str, head_sink_tn = head_sink_str_sink_tn_tuple
        merged_tn = head_sink_tn.map(lambda r: (head_sink_str, r))
        merged_tn._name_str = f"sink_{head_sink_str}"
        #
        # We need this little factory to avoid unwanted variable shadowing for sink_str in the loop below.
        def get_map_fun(sink_str):
            return lambda r: (sink_str, r)
        #
        for sink_str, sink_tn in tail_sink_str_sink_tn_tuple_list:
            map_tn = sink_tn.map(get_map_fun(sink_str))
            map_tn._name_str = f"sink_{sink_str}"
            merged_tn = merged_tn.merge(map_tn)
        #
        sink_str_list = [sink_str for sink_str, _ in sink_str_sink_tn_tuple_list]
        merged_tn._sink_str_list = sink_str_list
        #
        return merged_tn

    def _create_evaluator(self):
        """Create a pydbsp evaluator for the topology node.
        
        Returns:
            evaluator: the newly created pydbsp evaluator of the topology"""
        evaluator = Evaluator(
            circuit=Circuit(),
            storage=DictStorage(),
            ctx=ComputeCtx(lattice=dbsp_time(2)),
            group=ZSetAddition())
        #
        return evaluator

    @staticmethod
    def build(*sink_tn_tuple):
        """Build: Merge the sinks and build the pydbsp circuit for the topology.
        
        Args:
            *sink_tn_tuple: one or more sink tn to build
        Returns:
            built_tn: the built topology node"""
        if sink_tn_tuple is None:
            raise Exception("At least one sink node required.")
        #
        merged_tn = TopologyNode._merge_sinks(*sink_tn_tuple)
        #
        def _reset_fun():
            evaluator = merged_tn._create_evaluator()
            #
            merged_tn.__foreach(lambda tn: tn._build_fun(evaluator))
        #
        _reset_fun()
        #
        merged_tn._reset_fun = _reset_fun
        #
        built_tn = merged_tn
        #
        return built_tn

    def reset(self):
        """Rebuild the circuit from scratch, clearing all state."""
        if self._reset_fun is None:
            raise Exception("Not built yet.")
        #
        self._reset_fun()

    # Input

    def push(self, source_str_input_any_list_dict):
        """Feed new input r into one or more named sources.
        
        Args:
            source_str_input_any_list_dict: a dictionary mapping source names to input lists to push"""
        source_str_source_tn_dict = self.get_source_nodes()
        #
        for source_str, source_tn in source_str_source_tn_dict.items():
            input_any_list = source_str_input_any_list_dict.get(source_str, [])
            #
            input_nodeId = source_tn._output_nodeId
            #
            zSet = source_tn._to_zSet_fun(input_any_list, self._pack_fun)
            #
            self._evaluator.push(input_nodeId, zSet)

    @staticmethod
    def _from_records(r_w_tuple_list, pack_fun):
        """Pack (r, w) pairs into a Z-set.
        
        Args:
            r_w_tuple_list: list of (r, w) tuples
            pack_fun: function serializing a r to bytes
        Returns:
            zSet: the Z-set"""
        zSet = ZSet({pack_fun(r): w for r, w in r_w_tuple_list})
        #
        return zSet

    @staticmethod
    def from_records(r_list, pack_fun):
        """Pack plain r (implicit w=1) into a Z-set.
        
        Args:
            r_list: list of plain r
            pack_fun: function serializing a r to bytes
        Return:
            zSet: the Z-set"""
        zSet = ZSet({pack_fun(r): 1 for r in r_list})
        #
        return zSet

    @staticmethod
    def from_debezium(m_list, pack_fun):
        """Pack Debezium change events into a Z-set.
        
        Args:
            m_list: list of Kafka-style m dicts
            pack_fun: function serializing a r to bytes
        Return:
            zSet: the Z-set"""
        inner_dict = {}
        for m in m_list:
            if m["value"]["op"] in ["c", "u"]:
                m1 = copy.deepcopy(m)
                m1["value"] = m["value"]["after"]
                inner_dict[pack_fun(m1)] = 1
            elif m["value"]["op"] == "d":
                m1 = copy.deepcopy(m)
                m1["value"] = m["value"]["before"]
                inner_dict[pack_fun(m1)] = -1
        #
        return ZSet(inner_dict)
    
    def to_zSet(self, to_zSet_fun):
        """Set the function used to convert input into a Z-set.
        
        Args:
            to_zSet_fun: function converting pushed input into a Z-set
        Return:
            tn: the modified topology node"""
        self._to_zSet_fun = to_zSet_fun
        #
        return self
    
    # Output

    def latest(self, gc_bool=True):
        """Process the input up to now and return the resulting latest output, optionally garbage-collecting the state.
        
        Args:
            gc_bool: if True, garbage-collect the state after processing (default = True)
        Return:
            output_any: the latest output"""
        zSet = self._evaluator.latest(self._output_nodeId)
        #
        if gc_bool:
            self._evaluator.compact()
        #
        unpacked_zSet = [(self._unpack_fun(packed_r), w) for packed_r, w in zSet.items()]
        #
        if self._sink_str_list is None:
            output_any = self._from_zSet_fun(unpacked_zSet)
        else:
            sink_str_unpacked_r_w_tuple_list_dict = {sink_str: [] for sink_str in self._sink_str_list}
            for (sink_str, unpacked_r), w in unpacked_zSet:
                sink_str_unpacked_r_w_tuple_list_dict[sink_str].append((unpacked_r, w))
            #
            output_any = {sink_str: self._from_zSet_fun(unpacked_r_w_tuple_list) for sink_str, unpacked_r_w_tuple_list in sink_str_unpacked_r_w_tuple_list_dict.items()}
        #
        return output_any

    def from_zSet(self, from_zSet_fun):
        """Set the function used to convert a Z-set into output.
        
        Args:
            from_zSet_fun: function converting the output Z-set into plain output
        Returns:
            tn: the modified topology node"""
        self._from_zSet_fun = from_zSet_fun
        #
        return self
            
    @staticmethod
    def _to_records(unpacked_r_w_tuple_list):
        """Identity: pass (r, w) pairs through unchanged.
        
        Args:
            unpacked_r_w_tuple_list: list of (r, w) tuples
        Returns:
            unpacked_r_w_tuple_list: identity"""
        return unpacked_r_w_tuple_list

    @staticmethod
    def to_records(unpacked_r_w_tuple_list):
        """Expand positive-w entries into an r list.
        
        Args:
            unpacked_r_w_tuple_list: list of (r, w) tuples
        Returns:
            r_list: the record list"""
        r_list = []
        for unpacked_r, w in unpacked_r_w_tuple_list:
            if w > 0:
                for _ in range(w):
                    r_list.append(unpacked_r)
        #
        return r_list

    @staticmethod
    def to_debezium(unpacked_r_w_tuple_list):
        """Turn weighted r into Debezium create/delete messages.
        
        Args:
            unpacked_r_w_tuple_list: list of (r, w) tuples
        Returns:
            m_list: the Debezium messages"""
        m_list = []
        for m, w in unpacked_r_w_tuple_list:
            if w > 0:
                for _ in range(w):
                    m1 = copy.deepcopy(m)
                    m1["value"]["before"] = None
                    m1["value"]["after"] = m["value"]
                    m1["value"]["op"] = "c"
                    m_list.append(m1)
            elif w < 0:
                for _ in range(-w):
                    m1 = copy.deepcopy(m)
                    m1["value"]["before"] = m["value"]
                    m1["value"]["after"] = None
                    m1["value"]["op"] = "d"
                    m_list.append(m1)
        #
        return m_list

    #

    def process(self, source_str_input_any_list_dict, gc_bool=True):
        """Push input and return the latest output in one call.
        
        Args:
            source_str_input_any_list_dict: a dictionary mapping source names to input lists to push
            gc_bool: if True, garbage-collect the state after processing (default = True)
        Return:
            output_any: the latest output"""
        self.push(source_str_input_any_list_dict)
        #
        output_any = self.latest(gc_bool)
        #
        return output_any

    ###
    # Helpers
    ###

    def __foreach(self, foreach_fun):
        """Visit every node from this topology node upstream and apply a side-effect.
        
        Args:
            foreach_fun: tn -> None function applied to every node"""
        visited_tn_set = set()
        #
        def ___foreach(tn):
            if tn not in visited_tn_set:
                visited_tn_set.add(tn)
                #
                for mother_tn in tn._mother_tn_set:
                    ___foreach(mother_tn)
                #
                foreach_fun(tn)
        #
        ___foreach(self)

    def __filter(self, filter_fun):
        """Collect nodes matching a predicate from this topology node upstream.
        
        Args:
            filter_fun: tn -> bool - filter predicate
        Returns:
            tn_set: the set of filtered topology nodes"""
        tn_set = set()
        visited_tn_set = set()
        #
        def __filter(tn):
            if tn in visited_tn_set:
                return
            visited_tn_set.add(tn)
            #
            if filter_fun(tn):
                tn_set.add(tn)
            #
            for mother_tn in tn._mother_tn_set:
                __filter(mother_tn)
        #
        __filter(self)
        #
        return tn_set

    ###
    # Getters
    ###

    def get_id(self):
        """Get this node's unique ID.

        Returns:
            id_str: the ID of this topology node"""
        return self._id_str
    
    def get_name(self):
        """Get this node's name.
        
        Returns:
            name_str: the name of this topology node"""
        return self._name_str

    def get_mothers(self):
        """Get this node's direct upstream nodes.
        
        Returns:
            mother_tn_set: the mothers of this topology node"""
        return self._mother_tn_set

    #

    def get_node_by_id(self, id_str):
        """Find a node in the graph from this topology node upstream by id.
        
        Args:
            id_str: node id to search for
        Returns:
            tn: the found topology node or None"""
        tn_set = self.__filter(lambda tn: tn._id_str == id_str)
        #
        if len(tn_set) == 0:
            return None
        else:
            return list(tn_set)[0]
    
    def get_node_by_name(self, name_str):
        """Find a node in the graph from this topology node upstream by name.
        
        Args:
            name_str: name to search for
        Returns:
            tn: the found topology node or None"""
        tn_set = self.__filter(lambda tn: tn._name_str == name_str)
        #
        if len(tn_set) == 0:
            return None
        else:
            return list(tn_set)[0]

    def get_source_nodes(self):
        """Get all source nodes from this topology node upstream, keyed by source name.
        
        Returns:
            name_str_tn_dict: a dictionary mapping the names of the sources to the corresponding topology nodes"""
        tn_set = self.__filter(lambda tn: tn._source_str is not None)
        #
        name_str_tn_dict = {tn._source_str: tn for tn in tn_set}
        #
        return name_str_tn_dict

    def get_sink_nodes(self):
        """Get all sink nodes from this topology node upstream, keyed by sink name.
        
        Returns:
            name_str_tn_dict: a dictionary mapping the names of the sinks to the corresponding topology nodes"""
        tn_set = self.__filter(lambda tn: tn._sink_str is not None)
        #
        name_str_tn_dict = {tn._sink_str: tn for tn in tn_set}
        #
        return name_str_tn_dict

    ###
    # State
    ###

    def get_state(self):
        """The underlying evaluator (circuit state).
        
        Return:
            evaluator: the evaluator of this topology node"""
        return self._evaluator

    def set_state(self, evaluator):
        """Replace the underlying evaluator (circuit state).
        
        Args:
            evaluator: pydbsp Evaluator running the circuit"""
        self._evaluator = evaluator

    #

    def load_state(self, serialized_state_bytes):
        """Restore state from bytes produced by save_state.
        
        Args:
            serialized_state_bytes: bytes previously produced by save_state"""
        evaluator = cloudpickle.loads(serialized_state_bytes)
        #
        self.set_state(evaluator)

    def save_state(self):
        """Serialize the current state to bytes.
        
        Returns:
            serialized_state_bytes: the current state of this topology node in bytes"""
        serialized_state_bytes = cloudpickle.dumps(self.get_state())
        #
        return serialized_state_bytes

    #

    def get_state_size(self):
        """Size in bytes of the serialized state.
        
        Returns:
            state_size_int: the size in bytes of the serialized state"""
        state_size_int = len(cloudpickle.dumps(self.get_state()))
        #
        return state_size_int

    #

    def topology(self, include_ids=False):
        """Render the graph from this topology node upstream as a nested expression string.
        
        Args:
            include_ids: if True, include node ids in the rendered graph
        Returns:
            str: the graph from this topology node upstream as a nested expression string"""
        def _topology(tn, visited_tn_set):
            if tn in visited_tn_set:
                if include_ids:
                    return f"REF:{tn._name_str}_{tn._id_str}"
                else:
                    return f"REF:{tn._name_str}"
            #       
            visited_tn_set.add(tn)
            #        
            include_ids_bool = include_ids
            mothers_int = len(tn._mother_tn_set)
            #
            mothers_list = list(tn._mother_tn_set)
            #
            match mothers_int:
                case 0:
                    if include_ids_bool:
                        return f"{tn._name_str}_{tn._id_str}"
                    else:
                        return tn._name_str
                case 1:
                    mother_str = _topology(mothers_list[0], visited_tn_set)
                    if include_ids_bool:
                        return f"{tn._name_str}_{tn._id_str}({mother_str})"
                    else:
                        return f"{tn._name_str}({mother_str})"
                case 2:
                    mother1_str = _topology(mothers_list[0], visited_tn_set)
                    mother2_str = _topology(mothers_list[1], visited_tn_set)
                    if include_ids_bool:
                        return f"{tn._name_str}_{tn._id_str}({mother1_str}, {mother2_str})"
                    else:
                        return f"{tn._name_str}({mother1_str}, {mother2_str})"
        #
        return _topology(self, set())

    def mermaid(self, include_ids=False):
        """Render the graph from this topology node upstream as a Mermaid diagram.
        
        Args:
            include_ids: if True, include node ids in the rendered graph
        Returns:
            str: the graph upstream from this topology node as a Mermaid diagram"""
        include_ids_bool = include_ids
        mermaid_edge_str_set = set()
        visited_tn_set = set()
        #
        def collect_edges(tn):
            if tn in visited_tn_set:
                return
            visited_tn_set.add(tn)
            #
            for mother_tn in tn._mother_tn_set:
                if include_ids_bool:
                    mermaid_edge_str = f"{mother_tn._id_str}[{mother_tn._name_str}_{tn._id_str}] --> {tn._id_str}[{tn._name_str}_{tn._id_str}]\n"
                else:
                    mermaid_edge_str = f"{mother_tn._id_str}[{mother_tn._name_str}] --> {tn._id_str}[{tn._name_str}]\n"
                #
                mermaid_edge_str_set.add(mermaid_edge_str)
                #
                collect_edges(mother_tn)
        #
        collect_edges(self)
        #
        mermaid_top_str = "```mermaid\ngraph TD\n"
        mermaid_edges_str = "".join(mermaid_edge_str_set)
        mermaid_bottom_str = "```"
        #
        return mermaid_top_str + mermaid_edges_str + mermaid_bottom_str

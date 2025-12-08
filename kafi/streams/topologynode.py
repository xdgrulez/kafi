from pydbsp.indexed_zset.functions.bilinear import join_with_index
from pydbsp.indexed_zset.operators.linear import LiftedIndex, LiftedLiftedIndex
from pydbsp.zset.operators.bilinear import DeltaLiftedDeltaLiftedJoin
from pydbsp.stream import step_until_fixpoint_and_return, step_until_fixpoint, Stream, StreamHandle
from pydbsp.zset.operators.linear import LiftedSelect, LiftedProject
from pydbsp.stream.operators.linear import LiftedStreamIntroduction, LiftedStreamElimination, Integrate, LiftedIntegrate
from pydbsp.stream.operators.bilinear import Incrementalize2
from pydbsp.stream import Stream, StreamHandle
from pydbsp.zset import ZSet, ZSetAddition

import json
import uuid

#

def source(name_str):
    stream = Stream(ZSetAddition())
    stream_handle = StreamHandle(lambda: stream)
    #
    def output_handle_function():
        return stream_handle
    #
    return TopologyNode(name_str, output_handle_function)

#
def message_dict_list_to_ZSet(message_dict_list):
    message_str_list = [json.dumps(message_dict) for message_dict in message_dict_list]
    zSet = ZSet({k: 1 for k in message_str_list})
    return zSet

#

class TopologyNode:
    def __init__(self, name_str, output_handle_function=lambda: None, step_function=lambda: None, daughter_topologyNode_list=[]):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._output_handle_function = output_handle_function
        self._step_function = step_function
        self._daughter_topologyNode_list = daughter_topologyNode_list
        #
        self.group = output_handle_function().get().group()

    def map(self, map_function):
        def map_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(map_function(message_dict))
        #
        liftedProject = LiftedProject(self._output_handle_function(), map_function1)
        #
        def output_handle_function():
            return liftedProject.output_handle()
        #
        def step_function():
            liftedProject.step()
        #
        return TopologyNode("map_op", output_handle_function, step_function, [self])

    def filter(self, filter_function):
        def filter_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            return filter_function(message_dict)
        #
        liftedSelect = LiftedSelect(self._output_handle_function(), filter_function1)
        #
        def output_handle_function():
            return liftedSelect.output_handle()
        #
        def step_function():
            liftedSelect.step()
        #
        return TopologyNode("filter_op", output_handle_function, step_function, [self])

    def join(self, other, on_function, projection_function):
        def on_function1(left_message_json_str, right_message_json_str):
            left_message_dict = json.loads(left_message_json_str)
            right_message_dict = json.loads(right_message_json_str)
            return on_function(left_message_dict, right_message_dict)
        #
        def projection_function1(left_message_json_str, right_message_json_str):
            left_message_dict = json.loads(left_message_json_str)
            right_message_dict = json.loads(right_message_json_str)
            return json.dumps(projection_function(left_message_dict, right_message_dict))
        #
        left_liftedStream = LiftedStreamIntroduction(self._output_handle_function())
        right_liftedStream = LiftedStreamIntroduction(other._output_handle_function())
        #
        deltaLiftedDeltaLiftedJoin = DeltaLiftedDeltaLiftedJoin(
            left_liftedStream.output_handle(),
            right_liftedStream.output_handle(),
            on_function1,
            projection_function1)
        #
        output_node = LiftedStreamElimination(deltaLiftedDeltaLiftedJoin.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            step_until_fixpoint(left_liftedStream)
            step_until_fixpoint(right_liftedStream)
            #
            step_until_fixpoint(deltaLiftedDeltaLiftedJoin)
            #
            step_until_fixpoint(output_node)
        #
        return TopologyNode("join_op", output_handle_function, step_function, [self, other])

    # def groupByAgg(self, by_function, agg_function):
    #     def by_function1(message_json_str):
    #         message_dict = json.loads(message_json_str)
    #         return json.dumps(by_function(message_dict))
    #     #
    #     def agg_function1(message_json_str):
    #         message_dict = json.loads(message_json_str)
    #         return json.dumps(agg_function(message_dict))
    #     #
    #     stream_handle = self._output_handle_function()
    #     integrated_stream = Integrate(stream_handle)
    #     lifted_integrated_stream = LiftedIntegrate(integrated_stream.output_handle())
    #     output_node = LiftedLiftedAggregate(lifted_integrated_stream.output_handle(), by_function1, agg_function1)
    #     #
    #     def output_handle_function():
    #         return output_node.output_handle()
    #     #
    #     def step_function():
    #         integrated_stream.step()
    #         lifted_integrated_stream.step()
    #         #
    #         step_until_fixpoint_and_return(output_node)
    #     #
    #     return TopologyNode("group_by_agg_op", output_handle_function, step_function, [self])

    def peek(self, peek_function):
        def peek_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            peek_function(message_dict)
            return message_json_str
        #
        liftedProject = LiftedProject(self._output_handle_function(), peek_function1)
        #
        def output_handle_function():
            return liftedProject.output_handle()
        #
        def step_function():
            liftedProject.step()
        #
        return TopologyNode("peek_op", output_handle_function, step_function, [self])
    
    #

    def name(self):
        return self._name_str

    def id(self):
        return self._id_str

    def output_handle_function(self):
        return self._output_handle_function

    def step_function(self):
        return self._step_function

    def daughters(self):
        return self._daughter_topologyNode_list    

    #

    def step(self):
        def traverse(topologyNode, stack):
            stack.append(topologyNode)
            for daughter in topologyNode.daughters():
                traverse(daughter, stack)
            return stack
        #
        stack = traverse(self, [])
        #
        while stack:
            topologyNode = stack.pop()
            topologyNode._step_function()
    
    def latest(self):
        return self._output_handle_function().get().latest()

    # def latest_until_fixed_point(self):
    #     latest_summed_zSet = ZSet({})
    #     latest_zSet = ZSet({})
    #     zSetAddition = ZSetAddition()
    #     while True:
    #         self.step()
    #         latest_zSet1 = self.latest()
    #         if latest_zSet1 != latest_zSet:
    #             if latest_zSet1.is_identity():
    #                 break
    #             latest_summed_zSet = zSetAddition.add(latest_summed_zSet, latest_zSet1)
    #             latest_zSet = latest_zSet1
    #         else:
    #             break
    #     return latest_summed_zSet

    #

    def get_node_by_id(self, id_str):
        def collect_node_dict(topologyNode, id_str_topologyNode_dict):
            if id_str == topologyNode.id():
                id_str_topologyNode_dict[id_str] = topologyNode
            else:
                for daughter_topologyNode in topologyNode.daughters():
                    collect_node_dict(daughter_topologyNode, id_str_topologyNode_dict)
        #
        id_str_topologyNode_dict = {}
        collect_node_dict(self, id_str_topologyNode_dict)
        #
        return id_str_topologyNode_dict[id_str]

    def get_node_by_name(self, name_str):
        def collect_node_dict(topologyNode, name_str_topologyNode_dict):
            if name_str == topologyNode.name():
                name_str_topologyNode_dict[name_str] = topologyNode
            else:
                for daughter_topologyNode in topologyNode.daughters():
                    collect_node_dict(daughter_topologyNode, name_str_topologyNode_dict)
        #
        name_str_topologyNode_dict = {}
        collect_node_dict(self, name_str_topologyNode_dict)
        #
        return name_str_topologyNode_dict[name_str]

    #

    def topology(self, include_ids=False):
        include_ids_bool = include_ids
        #
        daughters_int = len(self._daughter_topologyNode_list)
        match daughters_int:
            case 0:
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}"
                else:
                    return self._name_str
            case 1:
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}({self._daughter_topologyNode_list[0].topology()})"
                else:
                    return f"{self._name_str}({self._daughter_topologyNode_list[0].topology()})"
            case 2:
                if include_ids_bool:
                    return  f"{self._name_str}_{self._id_str}({self._daughter_topologyNode_list[0].topology()}, {self._daughter_topologyNode_list[1].topology()})"
                else:
                    return  f"{self._name_str}({self._daughter_topologyNode_list[0].topology()}, {self._daughter_topologyNode_list[1].topology()})"

    def mermaid(self, include_ids=False):
        def collect_nodes_and_edges(topologyNode, name_str_set, edge_str_list):
            if include_ids:
                name_str_set.add(f"{topologyNode.name()}_{topologyNode.id()}")
            else:
                name_str_set.add(topologyNode.name())
            #
            for daughter_topologyNode in topologyNode.daughters():
                if include_ids:
                    edge_str_list.append(f"{daughter_topologyNode.name()}_{topologyNode.id()} --> {topologyNode.name()}_{topologyNode.id()}")
                else:
                    edge_str_list.append(f"{daughter_topologyNode.name()} --> {topologyNode.name()}")
                #
                collect_nodes_and_edges(daughter_topologyNode, name_str_set, edge_str_list)
        #
        node_str_set = set()
        edge_str_list = []
        collect_nodes_and_edges(self, node_str_set, edge_str_list)
        #
        mermaid_str = ["graph TD"]
        mermaid_str.extend(f"    {name_str}" for name_str in node_str_set)
        mermaid_str.extend(f"    {edge_str}" for edge_str in edge_str_list)
        #
        return "\n".join(mermaid_str)

import random

import cloudpickle as pickle

from kafi.streams.topologynode import (
    get,
    update,
    sum,
    agg_tuple,
    source,
    message_dict_list_to_ZSet,
    zSet_to_message_dict_list_tuple
)

from test_topologynode_base import TestTopologyNodeBase
from test_streams_base import TestStreamsBase

#

class TestStreamsJamieBase(TestStreamsBase, TestTopologyNodeBase):
    def step(self, source_topologyNode, root_topologyNode, batch_size_int):
        message_dict_list = []
        for id_int in range(0, batch_size_int):
            message_dict = {"key": str(id_int),
                            "value": {"from_account": random.randint(0, 9),
                                    "to_account": random.randint(0, 9),
                                    "amount": 1}}
            message_dict_list.append(message_dict)
        #
        zSet = message_dict_list_to_ZSet(message_dict_list)
        source_topologyNode.output_handle_function().get().send(zSet)
        #
        root_topologyNode.step()
        #
        root_topologyNode.gc()

    #

    def process(self, source_topologyNode, root_topologyNode, steps_int, batch_size_int):
        coll_updated_output_dict_list = []
        coll_deleted_output_dict_list = []
        for i in range(steps_int):
            self.step(source_topologyNode, root_topologyNode, batch_size_int)
            #
            latest_zSet = root_topologyNode.latest()
            updated_output_dict_list, deleted_output_dict_list = zSet_to_message_dict_list_tuple(latest_zSet)
            coll_updated_output_dict_list += updated_output_dict_list
            coll_deleted_output_dict_list += deleted_output_dict_list
            #
            print()
            print(f"{i} - Latest: {root_topologyNode.latest()}")
            print(len(pickle.dumps(root_topologyNode)) / 1024)
        #
        return coll_updated_output_dict_list, coll_deleted_output_dict_list

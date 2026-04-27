import asyncio, random, threading

import cloudpickle as pickle

from kafi.streams.topologynode import (
    message_dict_list_to_ZSet,
    zSet_to_message_dict_list_tuple
)
from kafi.streams.streams import streams 

from test_topologynode_jamie_base import TestTopologyNodeJamieBase
from test_streams_base import TestStreamsBase

#

class TestStreamsJamieBase(TestStreamsBase, TestTopologyNodeJamieBase):
    async def generate(self, batch_size_int):
        message_dict_list = []
        for id_int in range(0, batch_size_int):
            message_dict = {"key": str(id_int),
                            "value": {"from_account": random.randint(0, 9),
                                    "to_account": random.randint(0, 9),
                                    "amount": 1}}
            message_dict_list.append(message_dict)
        #
        return message_dict_list

    #

    async def process(self, source_topologyNode, root_topologyNode, message_dict_list):
        def run(stop_thread=None):
            asyncio.run(streams([(c, t1), (c, t2)], root_topologyNode, c, t3, c, t4, stop_thread))
        #
        stop_thread = threading.Event()
        thread = threading.Thread(target=run, args=[stop_thread])
        thread.daemon = True
        thread.start()
        #
        await asyncio.sleep(8)
        #
        stop_thread.set()
        thread.join()

    #

    def process(self, source_topologyNode, root_topologyNode, steps_int, batch_size_int):
        coll_updated_output_dict_list = []
        coll_deleted_output_dict_list = []
        for i in range(steps_int):
            message_dict_list = self.generate(batch_size_int)
            #
            self.step(source_topologyNode, root_topologyNode, message_dict_list)
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

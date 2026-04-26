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

#

class TestTopologyNodeJamieBase(TestTopologyNodeBase):
    def get_topologyNode_tuple(self):
        transaction_str = "transactions"
        #
        transaction_source_topologyNode = source(transaction_str)
        #
        transaction_topologyNode = transaction_source_topologyNode.map(
            lambda x: {
                "from_account": x["from_account"],
                "to_account": x["to_account"],
                "amount": x["amount"]
            },
            profile_config_dict=None
        )
        #
        credits_topologyNode = transaction_topologyNode.group_by_agg(
            by_function_list=[get("to_account")],
            as_function=update("account"),
            agg_tuple_list=[
                agg_tuple(
                    select_function=get("amount"),
                    agg_function=sum,
                    as_function=update("credits")
                )
            ],
            profile_config_dict=None
        )
        #
        debits_topologyNode = transaction_topologyNode.group_by_agg(
            [get("from_account")],
            update("account"),
            [agg_tuple(get("amount"), sum, update("debits"))],
            profile_config_dict=None
        )
        #
        balance_topologyNode = credits_topologyNode.join2(
            debits_topologyNode,
            left_on_function=lambda l: l["account"],
            right_on_function=lambda r: r["account"],
            projection_function=lambda l, r: {
                "account": l["account"],
                "balance": l["credits"] - r["debits"]
            },
            profile_config_dict=None
        )
        #
        root_topologyNode = balance_topologyNode.agg(
            agg_tuple_list=[
                agg_tuple(
                    select_function=get("balance"),
                    agg_function=sum,
                    as_function=update("sum")
                )
            ],
            profile_config_dict=None
        )
        #
        return transaction_source_topologyNode, root_topologyNode

    #

    def generate(self, batch_size_int):
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

    def step(self, source_topologyNode, root_topologyNode, message_dict_list):
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

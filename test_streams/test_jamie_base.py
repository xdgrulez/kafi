import random

from kafi.streams.topologynode import (
    source,
    program
)

#

class TestJamieBase():
    def get_topology(self, transaction_source_str):
        program2D = program()
        #
        transaction_source_topologyNode = source(program2D, transaction_source_str)
        #
        transaction_topologyNode = transaction_source_topologyNode.map(
            lambda x: {
                "from_account": x["from_account"],
                "to_account": x["to_account"],
                "amount": x["amount"]
            }
        )
        #
        # credits_topologyNode = transaction_topologyNode.group_by_agg(
        #     by_function_list=[get("to_account")],
        #     as_function=update("account"),
        #     agg_tuple_list=[
        #         agg_tuple(
        #             select_function=get("amount"),
        #             agg_function=sum,
        #             as_function=update("credits")
        #         )
        #     ]
        # )
        # #
        # debits_topologyNode = transaction_topologyNode.group_by_agg(
        #     [get("from_account")],
        #     update("account"),
        #     [agg_tuple(get("amount"), sum, update("debits"))]
        # )
        # #
        # balance_topologyNode = credits_topologyNode.join(
        #     debits_topologyNode,
        #     left_on_function=lambda l: l["account"],
        #     right_on_function=lambda r: r["account"],
        #     projection_function=lambda l, r: {
        #         "account": l["account"],
        #         "balance": l["credits"] - r["debits"]
        #     }
        # )
        #
        root_topologyNode = transaction_topologyNode
        root_topologyNode.set_program(program2D)
        #
        return root_topologyNode

    #

    def generate(self, _, batch_size_int):
        message_dict_list = []
        for id_int in range(0, batch_size_int):
            message_dict = {"key": str(id_int),
                            "value": {"from_account": random.randint(0, 9),
                                      "to_account": random.randint(0, 9),
                                      "amount": 1}}
            message_dict_list.append(message_dict)
        #
        return message_dict_list

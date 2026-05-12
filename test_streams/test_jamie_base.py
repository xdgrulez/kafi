import random

from kafi.streams.topologynode import (
    get,
    Runner
)

#

class TestJamieBase():
    def get_runner(self, transaction_source_str):
        runner = Runner()
        #
        transaction_source_topologyNode = runner.source(transaction_source_str)
        #
        transaction_topologyNode = transaction_source_topologyNode.map(
            lambda x: {
                "from_account": x["from_account"],
                "to_account": x["to_account"],
                "amount": x["amount"]
            }
        )
        #
        credits_topologyNode = transaction_topologyNode.group_by_sum(
            get("to_account"),
            get("amount"),
            lambda x, y: {"account": x,
                          "credits": y}
        )
        #
        debits_topologyNode = transaction_topologyNode.group_by_sum(
            get("from_account"),
            get("amount"),
            lambda x, y: {"account": x,
                          "debits": y}
        )
        #
        balance_topologyNode = credits_topologyNode.join(
            debits_topologyNode,
            left_on_function=lambda l: l["account"],
            right_on_function=lambda r: r["account"],
            projection_function=lambda l, r: {
                "account": l["account"],
                "balance": l["credits"] - r["debits"]
            }
        )
        #
        root_topologyNode = balance_topologyNode.sum(
            get("balance"),
            lambda _, y: {"sum": y}
        )
        #
        runner.root(root_topologyNode)
        #
        return runner

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

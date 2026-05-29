import random

from kafi.streams.topologynode import (
    source
)

from jamie.transactions import TransactionGenerator

#

class TestJamieBase():
    def get_root_tn(self, transaction_source_str):
        transaction_source_tn = source(transaction_source_str)
        #
        transaction_tn = transaction_source_tn.map(
            lambda x: {
                "from_account": x["from_account"],
                "to_account": x["to_account"],
                "amount": x["amount"]
            }
        )
        #
        # credits_topologyNode = transaction_topologyNode.group_by_sum(
        #     get("to_account"),
        #     get("amount"),
        #     lambda x, y: {"account": x,
        #                   "credits": y}
        # )
        # #
        # debits_topologyNode = transaction_topologyNode.group_by_sum(
        #     get("from_account"),
        #     get("amount"),
        #     lambda x, y: {"account": x,
        #                   "debits": y}
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
        # #
        # root_topologyNode = balance_topologyNode.sum(
        #     select_function=get("balance"),
        #     output_function=lambda _, y: {"sum": y}
        # )
        root_tn = transaction_tn
        #
        root_tn.setup()
        #
        return root_tn

    #

    def init_generate(self, source_str):
        if source_str == "transactions":
            self.generator_dict[source_str] = TransactionGenerator()
        else:
            raise Exception(f"Only transactions supported: {source_str}")

    def generate(self, source_str, batch_size_int):
        message_dict_list = []
        #
        generator = self.generator_dict[source_str]
        #
        for _ in range(batch_size_int):
            record_dict = generator.generate_record()
            message_dict = {"key": None,
                            "value": record_dict}
            message_dict_list.append(message_dict)
        #
        return message_dict_list 

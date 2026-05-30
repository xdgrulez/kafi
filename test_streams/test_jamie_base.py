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
        credits_tn = transaction_tn.group_by_sum(
            lambda x: x["to_account"],
            lambda x: x["amount"],
            lambda x, y: {"account": x,
                          "credits": y}
        )
        #
        debits_tn = transaction_tn.group_by_sum(
            lambda x: x["from_account"],
            lambda x: x["amount"],
            lambda x, y: {"account": x,
                          "debits": y}
        )
        #
        balance_tn = credits_tn.join(
            debits_tn,
            lambda l, r: l["account"] == r["account"],
            lambda l, r: {
                "account": l["account"],
                "balance": l["credits"] - r["debits"]
            }
        )
        #
        root_tn = balance_tn.sum(
            lambda x: x["balance"],
            lambda _, y: {"sum": y}
        )
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

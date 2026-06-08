from kafi.streams.topologynode import source

#

def get_root_tn_jamie(transaction_source_str):
    transaction_source_tn = source(transaction_source_str)
    #
    transaction_tn = transaction_source_tn.map(
        lambda x: {
            "from_account": x["value"]["from_account"],
            "to_account": x["value"]["to_account"],
            "amount": x["value"]["amount"]
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
    balance_tn = credits_tn.join_equi(
        debits_tn,
        lambda l: l["account"],
        lambda r: r["account"],
        lambda l, r: {
            "account": l["account"],
            "balance": l["credits"] - r["debits"]
        }
    )
    #
    root_tn = balance_tn.sum(
        lambda x: x["balance"],
        lambda _, y: {"value":
                      {"total": y}}
    )
    #
    root_tn.build()
    #
    return root_tn

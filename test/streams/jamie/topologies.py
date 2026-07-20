from kafi.streams.topologynode import TopologyNode as Tn

#


def get_built_tn_jamie(get_source_tn_fun, get_sink_tn_fun):
    transaction_source_tn = get_source_tn_fun()
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
    sum_tn = balance_tn.sum(
        lambda x: x["balance"],
        lambda x: {"value":
                   {"total": x}}
    )
    #
    sink_tn = get_sink_tn_fun(sum_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

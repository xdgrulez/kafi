from kafi.streams.topologynode import TopologyNode as Tn

#


def get_built_tn_jamie(get_source_tn_fun, get_sink_tn_fun):
    transaction_source_tn = get_source_tn_fun()
    #
    transaction_tn = transaction_source_tn.map(
        lambda r: {
            "from_account": r["value"]["from_account"],
            "to_account": r["value"]["to_account"],
            "amount": r["value"]["amount"]
        }
    )
    #
    credits_tn = transaction_tn.group_by_sum(
        key_fun=lambda r: r["to_account"],
        value_fun=lambda r: r["amount"],
        project_fun=lambda key_int, sum_int: {"account": key_int,
                                              "credits": sum_int}
    )
    #
    debits_tn = transaction_tn.group_by_sum(
        key_fun=lambda r: r["from_account"],
        value_fun=lambda r: r["amount"],
        project_fun=lambda key_int, sum_int: {"account": key_int,
                                              "debits": sum_int}
    )
    #
    balance_tn = credits_tn.join(
        debits_tn,
        left_key_fun=lambda l_r: l_r["account"],
        right_key_fun=lambda r_r: r_r["account"],
        project_fun=lambda l_r, r_r: {"account": l_r["account"],
                                      "balance": l_r["credits"] - r_r["debits"]}
    )
    #
    sum_tn = balance_tn.sum(
        lambda r: r["balance"],
        lambda sum_int: {"value": {"total": sum_int}}
    )
    #
    sink_tn = get_sink_tn_fun(sum_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

from kafi.streams.topologynode import TopologyNode as Tn

#

def get_built_tn_wc(get_source_tn_fun, get_sink_tn_fun):
    source_tn = get_source_tn_fun()
    #
    split_tn = source_tn.flatmap(
        lambda r: [{"word": word_str,
                    "position": i,
                    "partition": r["partition"],
                    "offset": r["offset"]} for i, word_str in enumerate(r["value"].split())]
    ).distinct()
    #
    group_by_count_tn = split_tn.group_by_count(
        lambda r: r["word"],
        lambda key_str, count_int: {"value": {"word": key_str,
                                              "count": count_int}}
    )
    #
    sink_tn = get_sink_tn_fun(group_by_count_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

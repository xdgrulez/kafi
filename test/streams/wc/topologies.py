from kafi.streams.topologynode import TopologyNode as Tn

#

def get_built_tn_wc(get_source_tn_function, get_sink_tn_function):
    source_tn = get_source_tn_function()
    #
    split_tn = source_tn.flatmap(
        lambda x: [{"word": word_str,
                    "position": i,
                    "partition": x["partition"],
                    "offset": x["offset"]} for i, word_str in enumerate(x["value"].split())]
    ).distinct()
    #
    group_by_count_tn = split_tn.group_by_count(
        lambda x: x["word"],
        lambda x, y: {"value":
                      {"word": x,
                       "count": y}}
    )
    #
    sink_tn = get_sink_tn_function(group_by_count_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

from kafi.streams.topologynode import TopologyNode as Tn

#

def get_root_tn_wc(lines_source_str, wc_sink_str):
    source_tn = Tn.source(lines_source_str)
    #
    split_tn = source_tn.flatmap(
        lambda x: [{"word": word_str,
                    "partition": x["partition"],
                    "offset": x["offset"],
                    "position": i} for i, word_str in enumerate(x["value"].split())]
    ).distinct()
    #
    group_by_count_tn = split_tn.group_by_count(
        lambda x: x["word"],
        lambda x, y: {"value":
                      {"word": x,
                       "count": y}}
    )
    #
    root_tn = Tn.sink(wc_sink_str, group_by_count_tn)
    #
    root_tn.build()
    #
    return root_tn

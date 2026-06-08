from kafi.streams.topologynode import source

#

def get_root_tn_wc(lines_str):
    _source_tn = source(lines_str)
    #
    split_tn = _source_tn.explode(
        lambda x: [{"word": word_str,
                   "partition": x["partition"],
                   "offset": x["offset"],
                   "position": i} for i, word_str in enumerate(x["value"].split())]
    ).distinct()
    #
    root_tn = split_tn.group_by_count(
        lambda x: x["word"],
        lambda x, y: {"value": 
                        {"word": x,
                        "count": y}}
    )
    #
    root_tn.build()
    #
    return root_tn

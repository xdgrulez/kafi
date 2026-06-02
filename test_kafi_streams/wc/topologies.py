from kafi.streams.topologynode import source

#

def get_wc_root_tn(lines_str):
    _source_tn = source(lines_str)
    #
    split_tn = _source_tn.flatmap(
            lambda x: [{"word": word_str} for word_str in x["value"].split()]
    )
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

from kafi.streams.topologynode import (
    source
)

from wc.plaintext import PlainTextGenerator

from pydbsp.zset import ZSet

#

class TestWCBase():
    def get_root_tn(self, plain_text_str):
        _source_tn = source(plain_text_str)
        #
        split_tn = _source_tn.flatmap(
              lambda x: [{"word": word_str} for word_str in x["text"].split()]
        )
        #
        root_tn = split_tn.group_by_count(
            lambda x: x["word"],
            lambda x, y: {"word": x,
                          "count": y}
        )
        #
        root_tn.setup()
        #
        return root_tn

    #

    def init_generate(self, source_str):
        if source_str == "plain_text":
            self.generator_dict[source_str] = PlainTextGenerator()
        else:
            raise Exception(f"Only plain_text supported: {source_str}")

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

from kafi.kafi import *
from transformers import pipeline

classifier = pipeline('text-classification', model='ProsusAI/finbert', return_all_scores=True)

def get_fear_index(text_str):
    fear_index_int = 0
    if text_str:
        sentiment_dict_list = classifier(text_str)[0]
        for sentiment_dict in sentiment_dict_list:
            if sentiment_dict["label"] == "negative":
                fear_index_int = int(sentiment_dict["score"] * 100)
                break
    return fear_index_int

def map_function(message_dict):
    fear_index_int = get_fear_index(message_dict["value"]["text"])
    message_dict["value"]["sentiment"] = {"model": "finbert", "score": fear_index_int}
    return message_dict

c = Cluster("local")
c.consume_timeout(-1)
c.produce_batch_size(1)
c.rm("scored_protobuf")
schema_str = 'message Scored { required string datetime = 1; required string text = 2; message Source { required string name = 1; required string id = 2; required string user = 3; } required Source source = 3; message Sentiment { required string model = 1; required int32 score = 2; } required Sentiment sentiment = 4; }'
c.map_to("scraped_json", c, "scored_protobuf", map_function, source_value_type="json", target_value_type="protobuf", target_value_schema=schema_str)

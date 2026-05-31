import random

plain_text_str_list = ["hello kafi streams hello", "all streams lead to kafka", "join berlin buzzwords"]

class PlainTextGenerator:
    def generate_record(self):
        record_dict = {"text": random.choice(plain_text_str_list)}
        #
        return record_dict

if __name__ == "__main__":
    generator = PlainTextGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())

import random

line_str_list = ["hello kafi streams hello", "all streams lead to kafi", "join berlin buzzwords"]

class LineGenerator:
    def generate_record(self):
        line_str = random.choice(line_str_list)
        #
        return line_str

if __name__ == "__main__":
    generator = LineGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())

import random

class TransactionGenerator:
    def generate_record(self):
        record_dict = {"from_account": random.randint(0, 9),
                       "to_account": random.randint(0, 9),
                       "amount": 1}
        #
        return record_dict

if __name__ == "__main__":
    generator = TransactionGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())

import random

class TransactionGenerator:
    def generate_record(self):
        k_r = None
        #
        v_r = {"from_account": random.randint(0, 9),
               "to_account": random.randint(0, 9),
               "amount": 1}
        #
        return (k_r, v_r)

if __name__ == "__main__":
    generator = TransactionGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())

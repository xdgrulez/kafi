import random

from streams.datagen.constants.customers import customer_dict_list

#

# import sys
# sys.path.insert(1, ".")

#

#

class ShoeCustomerGenerator:
    def generate_record(self):
        record_dict = random.choice(customer_dict_list)
        #
        return record_dict

if __name__ == "__main__":
    generator = ShoeCustomerGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())

import random

from streams.datagen.constants.customers import customer_dict_list

#

class ShoeCustomerGenerator:
    def generate_record(self):
        k_r = None
        #
        v_r = random.choice(customer_dict_list)
        #
        return (k_r, v_r)

if __name__ == "__main__":
    generator = ShoeCustomerGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())

import random

from streams.datagen.constants.products import product_dict_list

#

class ShoeProductGenerator:
    def generate_record(self):
        k_r = None
        #
        v_r = random.choice(product_dict_list)
        #
        return (k_r, v_r)

if __name__ == "__main__":
    generator = ShoeProductGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())

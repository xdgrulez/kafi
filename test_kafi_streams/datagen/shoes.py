import random

from test_kafi_streams.datagen.constants.products import product_dict_list

product_dict_list = [
    {
        "id": "1",
        "brand": "Jones-Stokes"
    },
    {
        "id": "2",
        "brand": "Harvey-Kuhic"
    }]
#

class ShoeProductGenerator:
    def generate_record(self):
        record_dict = random.choice(product_dict_list)
        #
        return record_dict

if __name__ == "__main__":
    generator = ShoeProductGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())

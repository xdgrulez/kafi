import random

from test_kafi_streams.datagen.constants.customers import customer_dict_list

#
customer_dict_list = [
    {
        "id": "1",
        "first_name": "Cyrill"
    },
    {
        "id": "2",
        "first_name": "Anallese"
    },
    {
        "id": "3",
        "first_name": "Lucinda"
    }]
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

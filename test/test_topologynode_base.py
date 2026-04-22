import json, unittest

#

class TestTopologyNodeBase(unittest.TestCase):
    def setUp(self):
        print("Test:", self._testMethodName)

    def tearDown(self):
        print()
        print("---")
        print()
        #
        print("Updates:")
        for message_dict in self.updated_message_dict_list:
            print(json.dumps(message_dict, indent=2))
        #
        print()
        print("---")
        print()
        #
        print("Deletes:")
        for message_dict in self.deleted_message_dict_list:
            print(json.dumps(message_dict, indent=2))
        #
        print()
        print("---")
        print()
        #
        print(f"Number of updates: {len(self.updated_message_dict_list)}")
        print(f"Number of deletes: {len(self.deleted_message_dict_list)}")

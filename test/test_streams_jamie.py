import os, sys, threading

#

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_streams_jamie_base import TestStreamsJamieBase
from kafi.kafi import *

#

class TestStreamsJamie(TestStreamsJamieBase):
    def test_jamie(self):
        root_topologyNode = self.get_topology()
        #
        storage = Cluster("local")
        storage.recreate("transactions")
        storage.recreate("total")
        #
        self.produce([(storage, "transactions")], 100, 100)
        # thread1 = threading.Thread(target=self.produce, args=([(storage, "transactions")], 1, 10))
        thread2 = threading.Thread(target=self.process, args=([(storage, "transactions")], storage, "total", root_topologyNode))
        #
        # thread1.start()
        thread2.start()
        #
        # thread1.join()
        thread2.join()
        #
        message_dict_list = self.read(storage, "total")
        print(message_dict_list)

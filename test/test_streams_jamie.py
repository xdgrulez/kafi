import os, sys, threading

#

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_streams_base import TestStreamsBase
from test.test_jamie_base import TestJamieBase

from kafi.kafi import *

#

class TestStreamsJamie(TestStreamsBase, TestJamieBase):
    def test_jamie(self):
        root_topologyNode = self.get_topology()
        #
        storage = Cluster("local")
        storage.recreate("transactions")
        storage.recreate("total")
        #
        thread1 = threading.Thread(target=self.produce, args=([(storage, "transactions")], 100, 100))
        thread2 = threading.Thread(target=self.process, args=([(storage, "transactions")], storage, "total", root_topologyNode))
        #
        thread1.start()
        thread2.start()
        #
        time.sleep(10)
        #
        self.stop_function()
        #
        thread1.join()
        thread2.join()
        #
        message_dict_list = self.read(storage, "total")
        print(message_dict_list)

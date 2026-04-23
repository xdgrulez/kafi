import os
import sys

#

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_topologynode_jamie_base import TestTopologyNodeJamieBase
from kafi.kafi import *

#

class TestStreamsJamie(TestTopologyNodeJamieBase):
    async def test_jamie(self):


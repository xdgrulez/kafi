import os
import sys
import unittest

#

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

#

class TestSingleStorageBase(unittest.TestCase):
    def setUp(self):
        #
        print("Test:", self._testMethodName)

    def tearDown(self):
        pass

    #

    def test_sqlzoo_select_basics_1(self):
        print("Hallo")

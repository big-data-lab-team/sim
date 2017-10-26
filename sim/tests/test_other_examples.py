import subprocess, os
from unittest import TestCase

import sys


class TestOther_examples(TestCase):
    ## UTILITY METHODS
    def get_oexamples_dir(self):
        return os.path.join(os.path.dirname(__file__), "../../other-examples/")

    def get_data_dir(self):
        return os.path.join(os.path.dirname(__file__), "demo/ds001/sub-01/anat")

    ## TESTS
    def test_create_histo(self):
        actual_result=""
        command=[os.path.join(self.get_oexamples_dir()
                                       ,"gen_histo.py"),
                                       "-b","5",
                                       self.get_data_dir()]
        try:
            actual_result = subprocess.check_output(command)
        except:
            e = sys.exc_info()[0]
            print("<p>Error: %s</p>" % e )
        expected_result = "[(0.0, 6412336), (819.0, 26374), (1638.0, 163), (2457.0, 26), (3276.0, 13)]\n"
        self.assertTrue(actual_result == expected_result)


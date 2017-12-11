import subprocess, os
from logilab.common import pytest
from unittest import TestCase
import unittest
import sys


def hdfs_status():
    try:
        proc = subprocess.Popen("jps", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return = proc.returncode
        return s_return, s_output, s_err
        if ret != 0:
            return False
        else:
             if ('SecondaryNameNode' not in out) & ('DataNode' not in out) & ('NameNode' not in out):
                return False
             else:
                return True
    except:
        return False

IS_HDFS_UP = hdfs_status()
class TestOther_examples(TestCase):
    ## UTILITY METHODS
    def get_oexamples_dir(self):
        return os.path.join(os.path.dirname(__file__), "../../other-examples/")



    def get_data_dir(self):
        return os.path.join(os.path.dirname(__file__), "demo/ds001/sub-01/anat")

    def get_output_dir(self):

        return os.path.join(os.path.dirname(__file__), "../../output")

    def check_binarize_spark_content(self, output_path):
        if(os.path.isdir(output_path)):
            if os.listdir(output_path) != "":
                return True
                print "content exists"
            else:
                return False
        return True

    def run_spark_binarize(self, options=[]):
        expected_result = True
        command = [os.path.join(self.get_oexamples_dir(), "binarize_spark.py"),
                   "2457",
                   self.get_data_dir(),
                   self.get_output_dir(),
                   "4"]
        for option in options:
            command.append(option)
        try:
            subprocess.check_output(command)
        except:
            e = sys.exc_info()[0]
        actual_result = self.check_binarize_spark_content(self.get_output_dir())

        self.assertTrue(actual_result == expected_result)


    ## TESTS
    def create_histo(self):
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


    def test_binarize_spark(self):
        self.run_spark_binarize()

    @unittest.skipIf(IS_HDFS_UP==False, 'test_binarize_spark_hdfs Skipped as HDFS not running/installed.')
    def test_binarize_spark_hdfs(self):
        self.run_spark_binarize(options=["--hdfs"])

    @unittest.skipIf(IS_HDFS_UP==False, 'test_binarize_spark_hdfs_tmpfs Skipped as HDFS not running/installed.')
    def test_binarize_spark_hdfs_tmpfs(self):
        self.run_spark_binarize(options=["--hdfs","--tmpfs"])


    def test_binarize_spark_tmpfs(self):
        self.run_spark_binarize(options=["--tmpfs"])





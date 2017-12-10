import subprocess, os
from unittest import TestCase
import unittest
import sys


def hdfs_status():
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

IS_HDFS_UP = hdfs_status()
class TestOther_examples(TestCase):
    ## UTILITY METHODS
    def get_oexamples_dir(self):
        return os.path.join(os.path.dirname(__file__), "../../other-examples/")



    def get_data_dir(self):
        return os.path.join(os.path.dirname(__file__), "demo/ds001/sub-01/anat")

    def get_output_dir(self):
        return os.path.join(os.path.dirname(__file__), "output")

    def check_binarize_spark_content(self, output_path):
        if(os.path.isdir(self.get_output_dir())):
            if os.listdir(self.get_output_dir()) != "":
                return True
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


    def test_binarize_spark(self):
        self.run_spark_binarize()

    @unittest.skipIf(IS_HDFS_UP, True) #Execute hdfs test only if hdfs is up
    def test_binarize_spark_hdfs(self):
        self.run_spark_binarize(options=["--hdfs"])

    @unittest.skipIf(IS_HDFS_UP,True) #Execute hdfs test only if hdfs is up
    def test_binarize_spark_hdfs_tmpfs(self):
        self.run_spark_binarize(options=["--hdfs","--tmpfs"])


    def test_binarize_spark_tmpfs(self):
        self.run_spark_binarize(options=["--tmpfs"])






import os, pytest, subprocess, time
from unittest import TestCase

#def test_bids_validator():
 #   subprocess.call(["bids-validator","../demo/ds001"])

class TestSim(TestCase):

   ## UTILITY METHODS
   def get_sim_dir(self):
      return os.path.join(os.path.dirname(__file__),"../sim")
   
   def get_demo_dir(self):
      return os.path.join(os.path.dirname(__file__),"demo")
   
   def get_json_descriptor(self):
      return os.path.join(self.get_demo_dir(),"bids-app-example.json")

   def run_spark_bids(self,options=[]):
      output_name = "output"+str(time.time()*1000)
      command = [os.path.join(self.get_sim_dir()
                                                    ,"spark_bids.py"),
                                       self.get_json_descriptor(),
                                       os.path.join(self.get_demo_dir(),"ds001"),
                                       "output"]
      for option in options:
         command.append(option)
      stdout_string = subprocess.check_output(command,
                                      stderr=subprocess.STDOUT)
      self.assertTrue("ERROR" not in stdout_string)
      assert(os.path.isfile("output/avg_brain_size.txt"))
      with open(os.path.join("output","avg_brain_size.txt")) as f:
         output_content = f.read()
      self.assertTrue(output_content == "Average brain size is 830532 voxels")
      
   ## TESTS
   def test_demo_descriptor_valid(self):
      self.assertFalse(subprocess.call(["bosh-validate",
                                        self.get_json_descriptor()
                                        ,"-b"]))

   def test_spark_bids_no_option(self):
      self.run_spark_bids()

   def test_spark_bids_skip_group(self):
      self.run_spark_bids(["--skip-group-analysis"]) # just participant analysis
      self.run_spark_bids(["--skip-participant-analysis"]) # just the group analysis
      
      

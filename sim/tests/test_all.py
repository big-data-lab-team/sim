import os, pytest, random, subprocess, time
from unittest import TestCase
import boutiques

class TestSim(TestCase):

   ## UTILITY METHODS
   def get_sim_dir(self):
      return os.path.join(os.path.dirname(__file__),"..")
   
   def get_demo_dir(self):
      return os.path.join(os.path.dirname(__file__),"demo")
   
   def get_json_descriptor(self):
      return os.path.join(self.get_demo_dir(),"bids-app-example.json")

   def run_spark_bids(self,checkOutputGroup=True,options=[],correctBrainSize="830532",output_name=None):
      os.system("service docker start;")
      millitime = int(time.time()*1000)
      if not output_name:
         output_name = "output"+str(random.SystemRandom().randint(0,int(millitime)))
      command = [os.path.join(self.get_sim_dir()
                                                    ,"spark_bids.py"),
                                       self.get_json_descriptor(),
                                       os.path.join(self.get_demo_dir(),"ds001"),
                                       output_name]
      for option in options:
         command.append(option)
      try:
         stdout_string = subprocess.check_output(command,
                                                 stderr=subprocess.STDOUT)
      except subprocess.CalledProcessError as e:
         print(e.output.decode('utf8'))
         self.assertTrue(False,"Command-line execution failed {0}".format(str(command)))
      self.assertTrue(bytes("ERROR", "utf8") not in stdout_string)
      if checkOutputGroup:
         assert(os.path.isfile(os.path.join(output_name,"avg_brain_size.txt")))
         with open(os.path.join(output_name,"avg_brain_size.txt")) as f:
            output_content = f.read()
         content = "Average brain size is {0} voxels".format(correctBrainSize)
         self.assertTrue(output_content == content)
      
   ## TESTS
   def test_demo_descriptor_valid(self):
      self.assertFalse(boutiques.validate(self.get_json_descriptor(),"-b"))

   def test_spark_bids_no_option(self):
      self.run_spark_bids()

   def test_spark_bids_separate_analyses(self):
      self.run_spark_bids(options=["--skip-group-analysis"],checkOutputGroup=False,output_name="output") # just participant analysis
      self.run_spark_bids(options=["--skip-participant-analysis"],output_name="output") # just the group analysis

   def test_spark_bids_skip_participant(self):
      participant_file = "skip.txt"
      with open(participant_file,"w") as f:
         f.write("01")
      self.run_spark_bids(options=["--skip-participants", os.path.join(os.path.dirname(__file__),"skip.txt")],correctBrainSize="865472")
      
   #def test_spark_bids_hdfs(self):
   #   self.run_spark_bids(options=["--hdfs"])

      

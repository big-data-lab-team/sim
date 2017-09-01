import pytest
import subprocess
from unittest import TestCase

#def test_bids_validator():
 #   subprocess.call(["bids-validator","../demo/ds001"])

class TestSim(TestCase):
 
   def test_demo_descriptor_valid(self):
      self.assertFalse(subprocess.call(["bosh-validate","./demo/bids-app-example.json","-b"]))

   def test_spark_bids_no_option(self):
      self.assertFalse(subprocess.call(["../sim/spark_bids.py", "./demo/bids-app-example.json", "./demo/ds001", "output"]))
      # Check that results were correct
     # with open(os.path.join("output","") as f:
     #    template_string = f.read()

      #group_analysis_result = 

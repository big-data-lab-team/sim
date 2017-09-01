import pytest
import subprocess


#def test_bids_validator():
 #   subprocess.call(["bids-validator","../demo/ds001"])

def test_boutiques_validator():
   assert (subprocess.call(["bosh","../demo/bids-app-example.json"]))

#def test_sim():
   #subprocess.call(["spark_bids", "../demo/bids-app-example.json", "../demo/ds001", "output"])

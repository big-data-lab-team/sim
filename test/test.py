import pytest
import subprocess

def test_my_case(spark_context):
    assert (subprocess.call(["bids-validator","/home/lscaria/BigDataLab/sim/demo/bids-app-example.json"]) == True

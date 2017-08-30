#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from bids.grabbids import BIDSLayout
import argparse
import json
import os
import os.path as op
import subprocess

def create_RDD(bids_dataset_root,sc):
    layout = BIDSLayout(bids_dataset_root)
    return sc.parallelize(layout.get_subjects())

def write_invocation_file(bids_dataset, output_dir, participant_name, invocation_file):

    # Note: the invocation file format will change soon
    
    # Creates invocation object
    invocation = {}
    invocation["inputs"] = [ ]
    invocation["inputs"].append({"bids_dir": bids_dataset})
    invocation["inputs"].append({"output_dir_name": output_dir})
    invocation["inputs"].append({"analysis_level": "participant"})
    invocation["inputs"].append({"participant_label": participant_name})
    json_invocation = json.dumps(invocation)

    # Writes invocation
    with open(invocation_file,"w") as f:
        f.write(json_invocation)
        f.close()

def run_bids_app(bids_dataset, participant_name):
    
    # TODO: put the descriptor as argument
    path, fil = op.split(__file__)
    boutiques_descriptor = op.join(path, "bids-app-example.json")

    # Define output dir
    output_dir = "./output-{0}".format(participant_name)
    
    # Writes invocation
    invocation_file = "./invocation-{0}.json".format(participant_name)
    write_invocation_file(bids_dataset, output_dir, participant_name, invocation_file)
    
    run_command = "localExec.py {0} -i {1} -e".format(boutiques_descriptor, invocation_file)

    subprocess.check_output(run_command, shell=True, stderr=subprocess.STDOUT)
    
    return (participant_name, os.path.abspath(output_dir))

def main():
    # Spark initialization
    conf = SparkConf().setAppName("log_analyzer").setMaster("local")
    sc = SparkContext(conf=conf)
    
    parser=argparse.ArgumentParser()
    parser.add_argument("bids_dataset", help="BIDS dataset to be processed")
    args=parser.parse_args()
    bids_dataset = args.bids_dataset
    
    rdd = create_RDD(bids_dataset,sc)

    print(rdd.map(lambda x: run_bids_app(bids_dataset,x)).collect())
     
# Execute program
if  __name__ == "__main__":
    main()

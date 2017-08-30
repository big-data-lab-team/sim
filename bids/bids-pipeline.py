#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from bids.grabbids import BIDSLayout
import argparse
import os
import json
import os.path as op
import subprocess

def create_RDD(bids_dataset_root,sc):
    layout = BIDSLayout(bids_dataset_root)
    return sc.parallelize(layout.get_subjects())

def list_files_by_participant(bids_dataset, participant_name):
    array = []
    os.chdir(bids_dataset)
    for root, dirs, files in os.walk(bids_dataset):
       for file in files:
          if file.startswith("sub-{0}".format(participant_name)):
             array.append(file)
    return array
def run_bids_app(bids_dataset, participant_name):
    
    # TODO: put the descriptor as argument
    path, fil = op.split(__file__)
    boutiques_descriptor = op.join(path, "bids-app-example.json")

    # Creates invocation
    output_dir = "./output"
    invocation = {}
    obj = {}
    obj["bids_dir"] = bids_dataset
    obj["output_dir_name"] = output_dir
    obj["analysis_level"] = "participant"
    obj["participant_label"] = participant_name
    invocation["inputs"] = [ obj ]
    json_invocation = json.dumps(invocation)

    # Writes invocation
    invocation_file = "./invocation.json"
    with open(invocation_file,"w") as f:
        f.write(json_invocation)
        f.close()

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
    rdd.map(lambda x: list_files_by_participant(bids_dataset,x))
    
    print(rdd.map(lambda x: list_files_by_participant(bids_dataset,x)).collect())
    
    print(rdd.map(lambda x: run_bids_app(bids_dataset,x)).collect())
     
# Execute program
if  __name__ == "__main__":
    main()

#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from bids.grabbids import BIDSLayout
import argparse
import os, errno
import json
import os.path as op
import subprocess
import tarfile
import shutil

def create_RDD(bids_dataset_root,sc):
    layout = BIDSLayout(bids_dataset_root)
    return sc.parallelize(layout.get_subjects())

def create_subject_RDD(bids_dataset_root, sc, sub_dir='tar_subjects'):
    layout = BIDSLayout(bids_dataset_root)

    try:
        os.makedirs(sub_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    
    for sub in layout.get_subjects():
        files = layout.get(subject=sub)

        with tarfile.open("tar_subjects/sub-{0}.tar".format(sub), "w") as tar:
            for f in files:
                tar.add(f.filename)
    
    return sc.binaryFiles("tar_subjects")


def list_files_by_participant(bids_dataset, participant_label):
    array = []
    for root, dirs, files in os.walk(bids_dataset):
      for file in files:
        if file.startswith("sub-{0}".format(participant_label)):
          array.append(file)
    return array

def pretty_print(participant_label, output_path, log, returncode):
    if returncode == 0:
        print(" [ SUCCESS ] subj-{0} - {1}".format(participant_label, output_path))
    else:
        print(" [ ERROR ({0}) ] subj-{1} - {2} - {3}".format(returncode, participant_label, output_path, log))

def write_invocation_file(bids_dataset, output_dir, participant_label, invocation_file):

    # Note: the invocation file format will change soon
    
    # Creates invocation object
    invocation = {}
    invocation["inputs"] = [ ]
    invocation["inputs"].append({"bids_dir": bids_dataset})
    invocation["inputs"].append({"output_dir_name": output_dir})
    invocation["inputs"].append({"analysis_level": "participant"})
    invocation["inputs"].append({"participant_label": participant_label})
    json_invocation = json.dumps(invocation)

    # Writes invocation
    with open(invocation_file,"w") as f:
        f.write(json_invocation)

def get_bids_dataset(data, subject_label):

    filename = 'sub-{0}.tar'.format(subject_label)    
    foldername = 'sub-{0}'.format(subject_label)

    # Save participant byte stream to disk
    with open(filename, 'w') as f:
        f.write(data)
    
    # Now extract data from tar
    tar = tarfile.open(filename)
    tar.extractall(path=foldername)
    tar.close()

    os.remove(filename)

    return subject_label
    

def run_bids_app(boutiques_descriptor, data, participant_label):
    

    bids_dataset = get_bids_dataset(data, participant_label)

    # Define output dir
    output_dir = "./output-{0}".format(participant_label)
    
    # Writes invocation
    invocation_file = "./invocation-{0}.json".format(participant_label)
    write_invocation_file(bids_dataset, output_dir, participant_label, invocation_file)

    # Runs command and returns results and logs
    run_command = "./localExec.py {0} -i {1} -e -d".format(boutiques_descriptor, invocation_file)
    result = None
    try:
        log = subprocess.check_output(run_command, shell=True, stderr=subprocess.STDOUT)
        result = (participant_label, os.path.abspath(output_dir), log, 0)
    except subprocess.CalledProcessError as e:
        result = (participant_label, os.path.abspath(output_dir), e.output, e.returncode)
    os.remove(invocation_file)
    shutil.rmtree('sub-{0}'.format(participant_label))
    return result

def main():
    # Spark initialization
    conf = SparkConf().setAppName("BIDS pipeline")
    sc = SparkContext(conf=conf)
    
    parser=argparse.ArgumentParser()
    parser.add_argument("bids_dataset", help="BIDS dataset to be processed")
    args=parser.parse_args()
    bids_dataset = args.bids_dataset

    # TODO: put the descriptor as argument
    path, fil = op.split(__file__)
    boutiques_descriptor = op.join(os.path.abspath(path), "bids-app-example.json")

    #rdd = create_RDD(bids_dataset,sc)
    rdd = create_subject_RDD(bids_dataset,sc)
    

    # Uncomment to get a list of files by subject 
    # print(rdd.map(lambda x: list_files_by_participant(bids_dataset,x)).collect())
    
    # Map step
    mapped = rdd.map(lambda x: run_bids_app(boutiques_descriptor, x[1], x[0].split('-')[-1][:-4]))

    # Display results
    for (participant_label, output_path, log, returncode) in mapped.collect():
        pretty_print(participant_label, output_path, log, returncode)
     
# Execute program
if  __name__ == "__main__":
    main()

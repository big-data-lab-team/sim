#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from bids.grabbids import BIDSLayout
import argparse

def create_RDD(bids_dataset_root,sc):
    layout = BIDSLayout(bids_dataset_root)
    return sc.parallelize(layout.get_subjects())

def main():
    # Spark initialization
    conf = SparkConf().setAppName("log_analyzer").setMaster("local")
    sc = SparkContext(conf=conf)
    
    parser=argparse.ArgumentParser()
    parser.add_argument("bids_dataset", help="BIDS dataset to be processed")
    args=parser.parse_args()
    
    rdd = create_RDD(args.bids_dataset,sc)
    
    print(rdd.collect())
    
# Execute program
if  __name__ == "__main__":
    main()

#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
import nibabel as nib
from gzip import GzipFile
from io import BytesIO
import numpy as np
import argparse
import os
import subprocess
from hdfs import Config
import sys

def copy_to_hdfs(x, output, client):
    filename = x[0].split('/')[-1]
    client.upload(output,os.path.join('/run/user/1000',filename), overwrite=True)
    subprocess.check_output("rm {0}".format(os.path.join('/run/user/1000',filename)), shell=True)
    return (x[0], 0)

def save_nifti(x):
    filename = x[0].split('/')[-1]
    im = nib.Nifti1Image(x[1][1], x[1][0])
    nib.save(im, os.path.join('/run/user/1000', filename))
    return filename

def binarize(x, threshold):
    filename = save_nifti(x)
    run_command = "sudo docker run -v /run/user/1000:/binarized_tmp --rm fsl-docker-img bin/fsl_bin.sh {0} {1}".format(filename, threshold)

    subprocess.check_output(run_command, shell=True, stderr=subprocess.STDOUT)

    return (x[0],result)

def get_data(x):
    fh = nib.FileHolder(fileobj=GzipFile(fileobj=BytesIO(x[1])))
    im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})
    return (x[0], (im.affine, im.get_data()))

def main():

    conf = SparkConf().setAppName("binarize nifti")
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')


    parser = argparse.ArgumentParser(description='Binarize images using FSL installed in a Docker container')
    parser.add_argument('threshold', type=int, help="binarization threshold")
    parser.add_argument('folder_path', type=str, help='folder path containing all of the splits')
    parser.add_argument('output_path', type=str, help='output folder path')

    args = parser.parse_args()
   
    print args.folder_path 
    client = Config().get_client('dev')

    nibRDD = sc.binaryFiles(args.folder_path)\
        .map(lambda x: get_data(x))\
        .map(lambda x: binarize(x, args.threshold))\
        .map(lambda x: copy_to_hdfs(x, args.output_path, client)).collect()



if __name__ == "__main__":
    main()

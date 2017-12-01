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
from  common_functions import directory_exists,tmpfs_directories, hdfs_status

def copy_to_hdfs(x, output, hdfs, tmpfs_path, client):
    # Content not being written to the file, apparently!
    filename = x[0].split('/')[-1]
    if not(directory_exists(tmpfs_path)):
        tmpfs_path = tmpfs_directories()
    if tmpfs_path != "":
        client.upload(output, os.path.join(tmpfs_path, filename), overwrite=True)
        subprocess.check_output("rm {0}".format(os.path.join(tmpfs_path,filename)), shell=True)
    else:
        print("ERROR# Temporary file system not found while uploading")
    return (x[0], 0)


def save_nifti(x, tmpfs_path):
    filename = x[0].split('/')[-1]
    im = nib.Nifti1Image(x[1][1], x[1][0])
    nib.save(im, os.path.join(tmpfs_path, filename))
    return filename


def binarize(x, threshold, tmpfs_path):
    if not(directory_exists(tmpfs_path)):
        tmpfs_path = tmpfs_directories()
    if tmpfs_path != "":
        filename = save_nifti(x, tmpfs_path)
        run_command = "sudo docker run -v {2}:/binarized_tmp --rm fsl-docker-img bin/fsl_bin.sh {0} {1}".format(filename, threshold, tmpfs_path)
        result = subprocess.check_output(run_command, shell=True, stderr=subprocess.STDOUT)
    else:
        print("ERROR# Temporary file system not found")

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
    parser.add_argument("tmpfs_path", type=str, help='tmpfs folder path')
    parser.add_argument("--hdfs", action='store_true', help="Use HDFS")
    args = parser.parse_args()
   
    print args.folder_path 
    client = Config().get_client('dev')

    nibRDD = sc.binaryFiles(args.folder_path)\
        .map(lambda x: get_data(x))\
        .map(lambda x: binarize(x, args.threshold, args.tmpfs_path))\
        .map(lambda x: copy_to_hdfs(x, args.output_path, args.hdfs, client)).collect()



if __name__ == "__main__":
    main()

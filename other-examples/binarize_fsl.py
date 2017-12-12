#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
import nibabel as nib
from gzip import GzipFile
from io import BytesIO
import argparse
import os
import subprocess
from hdfs import Config
from common_functions import directory_exists,get_tmpfs_directory, hdfs_status

def copy_to_fs(x, output, hdfs, tmpfs_path):
    client = Config().get_client('dev')
    filename = x[0].split('/')[-1]
    tmpfs_path = validate_tmpfs_path(tmpfs_path)
    if hdfs & hdfs_status():
        client.upload(output, os.path.join(tmpfs_path, filename), overwrite=True)
        subprocess.check_output("rm {0}".format(os.path.join(tmpfs_path, filename)), shell=True)
    else:
        if hdfs:
            print "EXCEPTION# Writing to local file system as HDFS is not up."
        with open(os.path.join(tmpfs_path, filename), "w") as input_file:
            with open(os.path.join(output, filename), "w") as output_file:
                for line in input_file:
                    if "ROW" in line:
                        output_file.write(line)
        subprocess.check_output("rm {0}".format(os.path.join(tmpfs_path, filename)), shell=True)

    return (x[0], 0)


def save_nifti(x, tmpfs_path):
    filename = x[0].split('/')[-1]
    im = nib.Nifti1Image(x[1][1], x[1][0])
    nib.save(im, os.path.join(tmpfs_path, filename))
    return filename


def binarize(x, threshold, tmpfs_path):
    tmpfs_path = validate_tmpfs_path(tmpfs_path)
    filename = save_nifti(x, tmpfs_path)
    run_command = "sudo docker run -v {2}:/binarized_tmp --rm fsl-docker-img bin/fsl_bin.sh {0} {1}".format(
        filename, threshold, tmpfs_path)
    result = subprocess.check_output(run_command, shell=True, stderr=subprocess.STDOUT)
    return (x[0], result)


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
    parser.add_argument("--hdfs", action='store_true', help="Use HDFS")
    parser.add_argument("-tmpfs",type=str, help='tmpfs folder path')

    args = parser.parse_args()

    nibRDD = sc.binaryFiles(args.folder_path) \
        .map(lambda x: get_data(x)) \
        .map(lambda x: binarize(x, args.threshold, args.tmpfs)) \
        .map(lambda x: copy_to_fs(x, args.output_path, args.hdfs, args.tmpfs)).collect()


def validate_tmpfs_path(tmpfs_path):
    if tmpfs_path is None:
        return os.getcwd() #returning current working directory
    if (not directory_exists(tmpfs_path)):
        tmpfs_path = get_tmpfs_directory()
        if tmpfs_path == "":
            tmpfs_path = os.getcwd() #returning current working directory
    return tmpfs_path

if __name__ == "__main__":
    main()

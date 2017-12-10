#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
import nibabel as nib
from gzip import GzipFile
from io import BytesIO
import numpy as np
import argparse
import os
from hdfs import Config
from common_functions import hdfs_status


def save_nifti(x, output, hdfs, client=None):
    client = Config().get_client('dev')
    filename = x[0].split('/')[-1]
    im = nib.Nifti1Image(x[1][1], x[1][0])
    if hdfs & hdfs_status():
        nib.save(im, filename)
        client.upload(output, filename, overwrite=True)
    else:
        if hdfs:
            print "EXCEPTION# Writing to local file system as HDFS is not up."
        with open(os.path.join(output, filename), "w") as file1:
            file1.write(str(im))
    return (x[0], 0)


def binarize(x, threshold):
    return (x[0], (x[1][0], np.where(x[1][1] > threshold, np.iinfo(x[1][1].dtype).max, 0)))


def binarize_and_save(x, threshold, output,hdfs, client=None):
    bin_data = np.where(x[1][1] > threshold, np.iinfo(x[1][1].dtype).max, 0)
    return save_nifti((x[0], (x[1][0], bin_data)), output, hdfs, client)


def get_data(x):
    fh = nib.FileHolder(fileobj=GzipFile(fileobj=BytesIO(x[1])))
    im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})
    return (x[0], (im.affine, im.get_data()))


def main():
    conf = SparkConf().setAppName("binarize nifti")
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')

    parser = argparse.ArgumentParser(description='Binarize images')
    parser.add_argument('threshold', type=int, help="binarization threshold")
    parser.add_argument('input_path', type=str, help='folder path containing all of the splits')
    parser.add_argument('output_path', type=str, help='output folder path')
    parser.add_argument('num', type=int, choices=[2, 4, 6, 8], help='number of binarization operations')
    parser.add_argument("--hdfs", action='store_true', help="Use HDFS")
    parser.add_argument('--tmpfs', action='store_true', help='in memory computation')

    args = parser.parse_args()
    nibRDD = sc.binaryFiles(args.input_path) \
        .map(lambda x: get_data(x))

    if args.tmpfs:
        print "Performing in-memory computations"

        for i in xrange(args.num - 1):
            nibRDD = nibRDD.map(lambda x: binarize(x, args.threshold))
        nibRDD = nibRDD.map(lambda x: binarize_and_save(x, args.threshold, args.output_path, args.hdfs)).collect()

    else:
        print "Writing intermediary results to disk and loading from disk"

        binRDD = nibRDD.map(lambda x: binarize_and_save(x, args.threshold, args.output_path + "1", args.hdfs)).collect()

        for i in xrange(args.num - 1):
            binRDD = sc.binaryFiles(args.output_path + "1") \
                .map(lambda x: get_data(x)) \
                .map(lambda x: binarize_and_save(x, args.threshold, args.output_path + "1", args.hdfs)).collect()


if __name__ == "__main__":
    main()
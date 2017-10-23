#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from NipImage import NipImage
import argparse, os


def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return arg

def main():

    # Arguments parsing
    parser=argparse.ArgumentParser()

    # Required inputs
    parser.add_argument("boutiques_descriptor", help="Boutiques descriptor of the pipeline that will process the high-resolution image.")
    parser.add_argument("input_dir", help="The directory containing all the image chunks")
    parser.add_argument("index", metavar="FILE", type=lambda x: is_valid_file(parser, x), help="The index containing all the filenames to be processed")
    parser.add_argument("output_dir", help="Output directory.")
    
    # Optional inputs
    parser.add_argument("--threshold", type=float, help="the threshold value used to threshold the image")
    parser.add_argument("--binarise", action='store_true', help="Performs binarisation of the image")
    parser.add_argument("--output_id", default='output_filename', type=str, help="output_id in descriptor")
    args=parser.parse_args()

    nip_im = NipImage(args.boutiques_descriptor,
                           args.input_dir,
                           args.output_dir,
                           args.index,
                           { 'threshold': args.threshold,
                             'binarise': args.binarise,
                             'output_id': args.output_id})
    
    # Run!
    nip_im.run()

# Execute program
if  __name__ == "__main__":
    main()

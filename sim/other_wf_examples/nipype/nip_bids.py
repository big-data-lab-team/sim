#!/usr/bin/env python

from NipBIDS import NipBIDS
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
    parser.add_argument("bids_app_boutiques_descriptor", help="Boutiques descriptor of the BIDS App that will process the dataset.")
    parser.add_argument("bids_dataset", help="BIDS dataset to be processed.")
    parser.add_argument("output_dir", help="Output directory.")
    
    # Optional inputs
    parser.add_argument("--skip-participant-analysis", action = 'store_true', help="Skips participant analysis.")
    parser.add_argument("--skip-group-analysis", action = 'store_true', help="Skips groups analysis.")
    parser.add_argument("--skip-participants", metavar="FILE", type=lambda x: is_valid_file(parser, x), help="Skips participant labels in the text file.")
    args=parser.parse_args()

    nip_bids = NipBIDS(args.bids_app_boutiques_descriptor,
                           args.bids_dataset,
                           args.output_dir,
                           { 'skip_participant_analysis': args.skip_participant_analysis,
                             'skip_group_analysis': args.skip_group_analysis,
                             'skip_participants_file': args.skip_participants})
    

        
    # Run!
    nip_bids.run()
     
# Execute program
if  __name__ == "__main__":
    main()

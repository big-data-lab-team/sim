#!/usr/bin/env python
import sys
import os
import nibabel as nib
import  numpy as np
import numbers
import argparse


        
def mapper(bin_size, data_max, data_min):
    for line in sys.stdin:
            line = line.strip()
            line = line.split()
            slice_file = os.path.join(line[0])
            

            #open nifti image
            slice = nib.load(slice_file)
            data = slice.get_data().flat
     
            histogram = {}
            

            #iterate throught the voxels and add them to dictionary with an initial count of 1
            #if same voxel value is encountered again, increment the count of the voxel value by one
            for value in data:
                try:
                    voxel_value = value.item()
                except:
                    print('Error: Voxel value could not be cast to a python scalar')
                    sys.exit(0)
                
                #dictionary key will be in the form of (min, max) tuple. All values will be in min/max range, excluding the max, which will be the min value of the proceeding bin
                if isinstance(value, numbers.Number):
                    
                    if data_min is not None and data_max is not None and (voxel_value < data_min or voxel_value >= data_max):
                        continue

                    remainder = voxel_value % bin_size

                    #create bins. Bins intervals are of format [min, max[ (max is excluded)                    
                    if data_min is None or data_max is None: 
                        min_bin_value = voxel_value - remainder
                        max_bin_value = voxel_value + bin_size - remainder
                    else:
                        min_bin_value = voxel_value - remainder if voxel_value - remainder >= data_min and voxel_value - remainder <= data_max else data_min 
                        max_bin_value = voxel_value + bin_size -remainder if voxel_value + bin_size - remainder <= data_max and voxel_value + bin_size - remainder >= data_min  else data_max


                    key = round(min_bin_value, 3), round(max_bin_value, 3)

                    if key in histogram:
                        histogram[key] += 1
                    else:
                        histogram[key] = 1

            for key, value in histogram.items():
                print("%s\t%d" % (key,value))

def main():
    
    parser = argparse.ArgumentParser(description="Generate histogram of nifti-1 image")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("-nb", "--num_bins", help="The number of bins in your histogram (integer value), max value (float), min value (float)*. ", action='append', nargs='+')
    group.add_argument("-br", "--bin_range", help="The interval range of the bins (float), max value (float)*, min value(float)*", action='append', nargs='+')

    number_bins = None
    data_min = None
    data_max = None
    bin_size = 1

    try:
        args= parser.parse_args()
    except:
        parser.print_usage()
        sys.exit(0)

    if args.num_bins:
        try:
            number_bins = int(args.num_bins[0][0])
            data_max = float(args.num_bins[0][1])
            
            if len(args.num_bins[0]) > 2:
                data_min = float(args.num_bins[0][2])
            else:
                data_min = 0
        except:
            print("Error: --num_bins argument must be in the format int float float*")
            sys.exit(0)

        try:
            bin_size = round((data_max - data_min) / number_bins, 5)
        except:
            print('Error: --num_bins cannot be 0.')        

    elif args.bin_range:
        try:
            bin_size = float(args.bin_range[0][0])
            if len(args.bin_range[0]) == 2:
                data_max = float(args.bin_range[0][1])
                data_min = 0
            elif len(args.bin_range[0]) > 2:
                data_max = float(args.bin_range[0][1])
                data_min = float(args.bin_range[0][2])
        except:
            print("Error: --bin_range argument must be in the format float float* float*")

    
    try:
        if bin_size == 0:
            raise ValueError('bin_size cannot be 0')
        if data_min >= data_max:
            raise ValueError('min value cannot be greater or equal to max')
    except ValueError as error:
        print('Error:' +  error.args[0])

    mapper(bin_size, data_max, data_min)
                    
if __name__ == "__main__": main()


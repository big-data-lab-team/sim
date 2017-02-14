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
                    voxel_value = float(value.item())
                except:
                    print('Error: Voxel value could not be cast to a float')
                    sys.exit(1)
                #dictionary key will be in the form of (min, max) tuple. All values will be in min/max range, excluding the max, which will be the min value of the proceeding bin
                try:
                    
                    if (voxel_value < data_min or voxel_value > data_max):
                        continue

                    remainder = (voxel_value - data_min) % bin_size

                    #create bins. Bins intervals are of format [min, max[ (max is excluded)                    
                    min_bin_value = voxel_value - remainder if voxel_value - remainder >= data_min and voxel_value - remainder <= data_max else data_min 

                    key = round(min_bin_value, 3)

                    if key in histogram:
                        histogram[key] += 1
                    else:
                        histogram[key] = 1
                except:
                    print('Error: error occured while generating histogram')
                    sys.exit(1)
            
            for key, value in histogram.items():
                print("%s\t%d" % (key,value))

def main():
    
    parser = argparse.ArgumentParser(description="Generate histogram of nifti-1 image")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("-nb", "--num_bins", help="The number of bins in your histogram (integer value). ", type=int)
    group.add_argument("-br", "--bin_range", help="The interval range of the bins (float)", type=float)
    parser.add_argument("--min_val", help="The minimum voxel value of the image", type=float, required=True)
    parser.add_argument("--max_val", help="The maximum voxel value of the image", type=float, required=True)


    args=parser.parse_args()



    number_bins = args.num_bins
    bin_range = args.bin_range if args.bin_range else 1
    
    data_min = args.min_val
    data_max = args.max_val

    
    if number_bins:
        try:
            #added a '+1' such that the data_max would be included in result without creating an extra bin
            bin_range = round((data_max - data_min + 1) / number_bins, 3 )
        except:
            print('Error: --num_bins cannot be 0')
            sys.exit(1)



    
    try:
        if bin_range == 0:
            raise ValueError('bin_size cannot be 0')
        if data_min >= data_max:
            raise ValueError('min value cannot be greater or equal to max')
    except ValueError as error:
        print('Error:' +  error.args[0])
        sys.exit(1)
    
    mapper(bin_range, data_max, data_min)
                    
if __name__ == "__main__": main()


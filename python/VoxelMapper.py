import sys
import os
import nibabel as nib
import  numpy as np

for line in sys.stdin:
	line = line.strip()
	slice_file = os.path.join(line)
	
        #open nifti image
        slice = nib.load(slice_file)
	data = slice.get_data().flat
 
        histogram = {}
	    
        #iterate throught the voxels and add them to dictionary with an initial count of 1
        #if same voxel value is encountered again, increment the count of the voxel value by one
	for key in data:
            
            if key in histogram:
                histogram[key] += 1
            else:
                histogram[key] = 1

        for key, value in histogram.items():
            print("%s\t%d" % (key, value))
		


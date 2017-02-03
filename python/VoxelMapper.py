import sys
import os
import nibabel as nib
import  numpy as np

for line in sys.stdin:
	line = line.strip()
	slice_file = os.path.join("/data/test/2dslice/" + line)
	
        #open nifti image
        slice = nib.load(slice_file)
	data = slice.get_data()

        #iterate throught the voxels and assign a value of 1 to each voxel encountered
	for row in data:
		for key in row:
			value = 1
			print("%s\t%d" % (key, value))
		


# mapreduce
A set of MapReduce programs to process brain images

###Java implementation
To run:<br/>
<code>hadoop jar GetHistogram.jar GetHistogram <input file> <output folder></code>

Dependencies: niftijio - https://github.com/cabeen/niftijio


###Python implementation

To run:<br/>
<code>hadoop jar <hadoop streaming jar path> -input <input filename> -output <output directory> -mapper VoxelMapper.py -reduce VoxelCountReducer.py</code>


Dependencies: nibabel - http://nipy.org/nibabel/

---
Note: Input file must be a text file containing the absolute file paths to all nii.tar.gz images.
<br/> ex. filenames.txt
<br/> /home/usr/img1.nii.tar.gz
<br/> /home/usr/img2.nii.tar.gz

For the python implementation, the input images found in the input text file currently need to be 2d slices of the 3d image.


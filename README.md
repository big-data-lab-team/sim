# mapreduce
A set of MapReduce programs to process brain images



##Histogram generation

Program to generate the histogram of given nifti image. <br/>
Note: Data binning is not currently implemented and thus bin size is always 1.
 
###Java implementation
To run:<br/>
<code>hadoop jar GetHistogram.jar GetHistogram <input file> <output folder></code>

Dependencies: niftijio - https://github.com/cabeen/niftijio


###Python implementation

To run:<br/>
<code>hadoop jar < hadoop streaming jar path > -input < input filename > -output < output directory > -mapper "python VoxelMapper.py" -reducer "python VoxelCountReducer.py"</code>


Dependencies: nibabel - http://nipy.org/nibabel/

--
Note: Input file must be a text file containing the absolute file paths to all nii.tar.gz images.
<br/> ex. filenames.txt
<br/> /home/usr/img1.nii.tar.gz
<br/> /home/usr/img2.nii.tar.gz


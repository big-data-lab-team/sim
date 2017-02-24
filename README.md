# mapreduce
A set of MapReduce programs to process brain images



##Histogram generation

Program to generate the histogram of given nifti image. <br/>
 
###Java implementation
To run:<br/>
<code>hadoop jar GetHistogram.jar GetHistogram  &lt;input file&gt;  &lt;output folder&gt; [optional arguments]</code>

Dependencies: niftijio - https://github.com/cabeen/niftijio

Note: Data binning is not currently implemented for java thus bin size is always 1.

###Python implementation

To run:<br/>
<code>python gethisto.py -hj  &lt;hadoop streaming jar&gt; -i &lt;input file&gt; -o &lt;output folder&gt; [Optional arguments]</code>

Optional arguments:
* -nb (--num_bins) - the number of bins (int) in histogram output. If this option is selected, the bin_range argument cannot be set.
* -br (--bin_range) - the interval range (float) of bins. If this option is selected, the num_bins argument cannot be set.
* --min_val - the minimum value of the image (float). If not provided, the program will determine minimum value. In the instance that the minimum value provided is not the true minimum value of the image, the program will nevertheless start binning from the given minimum value, ignoring all voxel values less than the provided minimum.
* --max_val - the maximum value of the image (float). If not provided, the program will determine maximum value. In the instance that the maximum value provided is no the true maximum value of the image, the program will nevertheless end binning at the given maximum.
* --num_reducers - the number of reducers to be used in map/reduce operation (int)
* --num_mappers - the number of mappers to be used in map/reduce operation (int)



Dependencies: nibabel - http://nipy.org/nibabel/

--
Note: Input file must be a text file containing the absolute file paths to all nifti images.
<br/> ex. filenames.txt
<br/> /home/usr/img1.nii.tar.gz
<br/> /home/usr/img2.nii.tar.gz


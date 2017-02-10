# mapreduce
A set of MapReduce programs to process brain images



##Histogram generation

Program to generate the histogram of given nifti image. <br/>
 
###Java implementation
To run:<br/>
<code>hadoop jar GetHistogram.jar GetHistogram <input file> <output folder></code>

Dependencies: niftijio - https://github.com/cabeen/niftijio

Note: Data binning is not currently implemented for java thus bin size is always 1.

###Python implementation

To run:<br/>
<code>hadoop jar < hadoop streaming jar path > -input < input filename > -output < output directory > -mapper "python VoxelMapper.py [arguments]" -reducer "python VoxelCountReducer.py"</code>

Possible input arguments for the mapper:
<ul>
<li>-nb --num_bins : the number of bins contained within the histogram. This value must be in int format and must be followed by a float representing the maximum value of the dataset. Optionally, a minimum value can be inputted. If a minimum value is not supplied, a value of 0 is assumed to be the minimum.
<br/>
ex: <dl><dd><code> python VoxelMapper.py -nb 10 5 </code>, where 10 is the number of bins and 5 is the maximum value</dd>

<dd><code> python VoxelMapper.py -nb 10 500 10 </code>, where 10 is the number of bin 500 is the maximum value and 10 is the minimum value</dd>
</dl>
</li>
<li>-br --bin_range : the interval size contained within each bin. Optionally, a maximum and a minimum dataset value can be inputted. Note: If a minimum value is supplied, a maximum value must be as well. If neither minimum not maximum is provided, the program will assume the minimum is 0 and the maximum is greater than or equal to the maximum value in the dataset.  
<br/>
ex: <dl><dd><code> python VoxelMapper.py -br 2.5 5 </code>, where 2.5 is the interval range of bin (e.g.[0, 2.5[, [2.5, 5[) and 5 is the maximum value</dd>
<dd><code> python VoxelMapper.py -br 10 500 10 </code>, where 10 is the interval range (e.g. [0, 10[, [10, 20[) 500 is the maximum value and 10 is the minimum value</dd></dl>
</li>
</ul>

Dependencies: nibabel - http://nipy.org/nibabel/

--
Note: Input file must be a text file containing the absolute file paths to all nii.tar.gz images.
<br/> ex. filenames.txt
<br/> /home/usr/img1.nii.tar.gz
<br/> /home/usr/img2.nii.tar.gz


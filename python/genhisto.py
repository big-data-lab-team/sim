import argparse
import subprocess
import numpy as np
import tempfile
import sys
import os
from hdfsutils import HDFSUtils

def main():

    parser = argparse.ArgumentParser(description="Generate histogram of nifti-1 image")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("-nb", "--num_bins", help="The number of bins in your histogram (integer value). ", type=int)
    group.add_argument("-br", "--bin_range", help="The interval range of the bins (float)", type=float)
    parser.add_argument("--min_val", help="The minimum voxel value of the image", type=float)
    parser.add_argument("--max_val", help="The maximum voxel value of the image", type=float)
    

    parser.add_argument("--num_reducers", help="The number of reducers to be used", type=int)
    parser.add_argument("--num_mappers", help="The number of mappers to be used", type=int)

    parser.add_argument("-o", "--output", help="The output folder", type=str, default="HistogramOutput")
    
    required = parser.add_argument_group('required arguments')
    required.add_argument("-i", "--input_file", help="The input file", type=str, required=True)
    required.add_argument("-hj", "--hadoop_streaming_jar", help="The hadoop streaming jar", required=True)

    args = parser.parse_args()
    
    number_bins = args.num_bins
    bin_range = args.bin_range

    data_min = args.min_val
    data_max = args.max_val

    num_reducers = args.num_reducers
    num_mappers = args.num_mappers

    input_file = args.input_file
    output_folder = args.output

    hadoop_jar = args.hadoop_streaming_jar

    util = HDFSUtils()   
 
    # will get the min/max value of the image if min/max is not already provided
    if(data_min == None or data_max == None):

        temp_dir = tempfile.mkdtemp() if not util.is_local(output_folder) else 'file://' + os.getcwd()  + tempfile.mkdtemp()

        find_min_max_command = 'hadoop jar {0} -D mapreduce.job.reduces=0 -D mapreduce.job.map={1} -input {2} -output {3} -mapper "python minmaxmap.py" -file minmaxmap.py'.format(hadoop_jar, num_mappers, input_file, temp_dir)
        #run bash command
        if(subprocess.call(find_min_max_command, shell=True) != 0):
            print("Error occured while attempting to determine min/max")
            sys.exit(1)


        get_minmax_out = None

        #save minMax result to intermediate folder
        #should perhaps delete folder after result is extracted..
        if not util.is_local(temp_dir):
            get_minmax_out = 'hdfs dfs -cat {0}/* | sort -n '.format(temp_dir)
        else:
            get_minmax_out = 'cat {0}/* | sort -n '.format(temp_dir.replace('file://', '')) 
        
        #get mapreduce result
        result = subprocess.check_output(get_minmax_out, shell=True)
            
        try:
            data_min = float(result.split()[0]) if data_min is None else data_min
            data_max = float(result.split()[len(result.split()) - 1]) if data_max is None else data_max
        except:
            print('Error: Output produced does not contain a min and max value')
            sys.exit(1)                

    args_for_mapper = '-nb {0}'.format(number_bins) if number_bins is not None else ''
    args_for_mapper += '-br {0}'.format(bin_range) if bin_range is not None else ''
    args_for_mapper += ' --min_val {0} --max_val {1}'.format(data_min, data_max)

    hadoop_streaming_args = '-D mapreduce.job.reduces={0}'.format(num_reducers) if num_reducers is not None else ''
    hadoop_streaming_args += ' -D mapreduce.job.map={0}'.format(num_mappers) if num_mappers is not None else ''
    

    histogram_command = 'hadoop jar {0} {1} -input {2} -output {3} -mapper "python voxel_mapper.py {4}" -reducer "python voxel_count_reducer.py" -file voxel_mapper.py -file voxel_count_reducer.py'.format(hadoop_jar, hadoop_streaming_args, input_file, output_folder, args_for_mapper)

    #create histogram
    subprocess.call(histogram_command, shell=True)

if __name__ == "__main__": main()

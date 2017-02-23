#!/usr/bin/env python
import sys
import os
import nibabel as nib
import numpy as np
from io import BytesIO
from nibabel import FileHolder, Nifti1Image
from gzip import GzipFile
from hdfs import Config

#get largest and smallest possible float
max = sys.float_info.min
min = sys.float_info.max

for line in sys.stdin:
    line = line.strip()
    line = line.split()
    slice_file = os.path.join(line[0])
    slice = None
    
    #TODO:will change to a temp file or overwrite input file. File  to be passed to mapper as an argument instead of fixed filename
    #filename_list = "filenames_updated.txt"

    #create a temp file that will store filename, filesystem (i.e. local or hdfs), and if gzipped or not
    #load nifti image from hdfs
    if not os.path.exists(slice_file):
        fh = None

        client = Config().get_client()
        with client.read(slice_file) as reader:
            #temporary non-recommended solution to determining if file is compressed. 
            if '.gz' in slice_file[-3:]: 
                fh = FileHolder(fileobj=GzipFile(fileobj=BytesIO(reader.read())))
                file_specs =  "{0}\tTrue\tTrue\n".format(slice_file)
            else:
                fh = FileHoder(fileobj=BytesIO(reader.read()))
                file_specs =  "{0}\tTrue\tFalse\n".format(slice_file)
        slice =  Nifti1Image.from_file_map({'header':fh, 'image':fh})
        try:
            client.write(filename_list, data=file_specs)
     d  except:
           client.write(filename_list, data=file_specs, append=True)
 
    #load from local filesystem
    else:
        slice = nib.load(slice_file)
        with open(os.getcwd() + '/' + filename_list, 'a') as f:
            f.write("{0}\tFalse\tFalse\n".format(slice_file))
    data = slice.get_data().flat


    for value in data:
        try:
            voxel = float(value.item())
        
        except:
            print('Error: Voxel value could not be cast to a float')
            sys.exit(1)

        if voxel > max:
            max = voxel
        if voxel < min:
            min = voxel        
    #write min and max to temp file
    print('{}\t '.format(min))
    print('{}\t '.format(max))
        
    

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

    #load nifti image
    if not os.path.exists(slice_file):
        fh = None

        client = Config().get_client()

        with client.read(slice_file) as reader:
            #temporary non-recommended solution to determining if file is compressed. 
            if 'gz' in slice_file[-2:]: 
                fh = FileHolder(fileobj=GzipFile(fileobj=BytesIO(reader.read())))
            else:
                fh = FileHoder(fileobj=GzipFile(fileobj=BytesIO(input_stream)))
        slice =  Nifti1Image.from_file_map({'header':fh, 'image':fh})
    else:
        slice = nib.load(slice_file)
    
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
        
    

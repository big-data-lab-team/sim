import nibabel as nib
from math import ceil
import magic
from gzip import GzipFile
from io import BytesIO
import sys
import numpy as np
from time import time
import os
import logging
from enum import Enum

start_indexes = {}
s_i_count = 0
class Merge(Enum):
    clustered = 0
    multiple = 1

class ClusteredWrites(Enum):
    SLICES = "slices"
    COMPLETE = "c_rows"
    INCOMPLETE = "i_rows"

class ImageUtils:
    """ Helper utility class for performing operations on images"""

    def __init__(self, filepath, first_dim=None, second_dim=None, third_dim=None, dtype=None, utils=None):
        # TODO: Review. not sure about these instance variables...

        """
        Keyword arguments:
        filepath                                : filepath to image
        first_dim, second_dim, third_dim        : the shape of the image. Only required if image needs to be generated
        dtype                                   : the numpy dtype of the image. Only required if image needs to be generated
        utils                                   : instance of HDFSUtils. necessary if files are located in HDFS
        """

        self.utils = utils
        self.filepath = filepath
        self.proxy = self.load_image(filepath)
        self.extension = split_ext(filepath)[1]
        

        self.header = None

        if self.proxy:
            self.header = self.proxy.header
        else:
            self.header = generate_header(first_dim, second_dim, third_dim, dtype)

        if dtype is not None:
            self.dtype = dtype
        else:
            self.dtype = self.header['datatype']

        self.affine = self.header.get_best_affine()
        self.header_size = self.header.single_vox_offset

        self.merge_types = {
            Merge.clustered: self.clustered_read,
            Merge.multiple: self.multiple_reads
        }

    def split(self, first_dim, second_dim, third_dim, local_dir, filename_prefix, hdfs_dir=None, copy_to_hdfs=False):
        """Splits the 3d-image into shapes of given dimensions
        Keyword arguments:
        first_dim, second_dim, third_dim: the desired first, second and third dimensions of the splits, respectively. 
        local_dir                       : the path to the local directory in which the images will be saved
        filename_prefix                 : the filename prefix
        hdfs_dir                        : the hdfs directory name should the image be copied to hdfs. If none is provided and
                                          copy_to_hdfs is set to True, the images will be copied to the HDFSUtils class' default
                                          folder
        copy_to_hdfs                    : boolean value indicating if the split images should be copied to HDFS. Default is False.

        """
        try:
            if self.proxy is None:
                raise AttributeError("Cannot split an image that has not yet been created.")
        except AttributeError as aerr:
            print 'AttributeError: ', aerr
            sys.exit(1)

        num_x_iters = int(ceil(self.proxy.dataobj.shape[2] / third_dim))
        num_z_iters = int(ceil(self.proxy.dataobj.shape[1] / second_dim))
        num_y_iters = int(ceil(self.proxy.dataobj.shape[0] / first_dim))

        remainder_x = self.proxy.dataobj.shape[2] % third_dim
        remainder_z = self.proxy.dataobj.shape[1] % second_dim
        remainder_y = self.proxy.dataobj.shape[0] % first_dim

        is_rem_x = is_rem_y = is_rem_z = False

        for x in range(0, num_x_iters):

            if x == num_x_iters - 1 and remainder_x != 0:
                third_dim = remainder_x
                is_rem_x = True

            for z in range(0, num_z_iters):

                if z == num_z_iters - 1 and remainder_z != 0:
                    second_dim = remainder_z
                    is_rem_z = True

                for y in range(0, num_y_iters):

                    if y == num_y_iters - 1 and remainder_y != 0:
                        first_dim = remainder_y
                        is_rem_y = True

                    x_start = x * third_dim
                    x_end = (x + 1) * third_dim

                    z_start = z * second_dim
                    z_end = (z + 1) * second_dim

                    y_start = y * first_dim
                    y_end = (y + 1) * first_dim

                    split_array = self.proxy.dataobj[y_start:y_end, z_start:z_end, x_start:x_end]
                    split_image = nib.Nifti1Image(split_array, self.affine)

                    imagepath = None

                    # TODO: fix this so that position saved in image and not in filename
                    # if the remaining number of voxels does not match the requested number of voxels, save the image with
                    # with the given filename prefix and the suffix: _<x starting coordinate>_<y starting coordinate>_<z starting coordinate>__rem-<x lenght>-<y-length>-<z length>
                    if is_rem_x or is_rem_y or is_rem_z:

                        y_length = y_end - y_start
                        z_length = z_end - z_start
                        x_length = x_end - x_start

                        imagepath = '{0}/{1}_{2}_{3}_{4}__rem-{5}-{6}-{7}.nii.gz'.format(local_dir, filename_prefix,
                                                                                         y_start, z_start, x_start,
                                                                                         y_length, z_length, x_length)
                    else:
                        imagepath = '{0}/{1}_{2}_{3}_{4}.nii.gz'.format(local_dir, filename_prefix, y_start, z_start,
                                                                        x_start)

                    nib.save(split_image, imagepath)

                    if copy_to_hdfs:
                        self.utils.copy_to_hdfs(imagepath, ovrwrt=True, save_path_to_file=True)
                    else:
                        with open('{0}/legend.txt'.format(local_dir), 'a+') as im_legend:
                            im_legend.write('{0}\n'.format(imagepath))

                    is_rem_z = False

            is_rem_y = False

    def split_multiple_writes(self, Y_splits, Z_splits, X_splits, out_dir, mem, filename_prefix="bigbrain", extension="nii"):
        """
        Split the input image into several splits, all share with the same shape
        For now only support .nii extension
        :param Y_splits: How many splits in Y-axis
        :param Z_splits: How many splits in Z-axis
        :param X_splits: How many splits in X-axis
        :param out_dir: Output Splits dir 
        :param mem: memory load each round
        :param filename_prefix: each split's prefix filename 
        :param extension: extension of each split
        :return: 
        """
        # calculate remainder based on the original image file
        Y_size, Z_size, X_size = self.header.get_data_shape()
        bytes_per_voxel = self.header['bitpix'] / 8
        original_img_voxels = X_size * Y_size * Z_size

        if X_size % X_splits != 0 or Z_size % Z_splits != 0 or Y_size % Y_splits != 0:
            raise Exception("There is remainder after splitting, please reset the y,z,x splits")
        x_size = X_size / X_splits
        z_size = Z_size / Z_splits
        y_size = Y_size / Y_splits

        # for benchmarking
        total_read_time = 0
        total_seek_time = 0
        total_write_time = 0
        total_seek_number = 0

        # get all split_names and write them to the legend file
        split_names = generate_splits_name(y_size, z_size, x_size, Y_size, Z_size, X_size, out_dir, filename_prefix, extension)
        legend_file = generate_legend_file(split_names, "legend.txt", out_dir)
        # generate all the headers for each split
        generate_headers_of_splits(split_names, y_size, z_size, x_size, self.header.get_data_dtype())
        # get all write offset of all split names
        print "Get split offsets..."
        split_offsets = get_offsets_of_all_splits(legend_file, Y_size, Z_size)
        # drop the remainder which is less than one slice
        # if mem is less than one slice, then set mem to one slice
        mem = mem  - mem  % (Y_size * Z_size * bytes_per_voxel) \
            if mem >= Y_size * Z_size * bytes_per_voxel \
            else Y_size * Z_size * bytes_per_voxel

        # get how many voxels per round
        voxels = mem / bytes_per_voxel
        next_read_index = (0, voxels - 1)

        # Core Loop:
        while True:
            next_read_offsets = (next_read_index[0] * bytes_per_voxel, next_read_index[1] * bytes_per_voxel + 1)
            print("From {} to {}".format(next_read_offsets[0], next_read_offsets[1]))
            from_x_index = index_to_voxel(next_read_index[0], Y_size, Z_size)[2]
            to_x_index = index_to_voxel(next_read_index[1] + 1, Y_size, Z_size)[2]

            st_read_time = time()
            data_in_range = self.proxy.dataobj[..., from_x_index: to_x_index]
            total_read_time += time() - st_read_time
            total_seek_number += 1

            # iterate all splits to see if in the read range:
            for split_name in split_names:
                in_range = check_in_range(next_read_offsets, split_offsets[split_name])
                if in_range:
                    split = Split(split_name)
                    # extract all the slices that in the read range
                    # X_index: index that in original image's coordinate system
                    # x_index: index that in the split's coordinate system
                    (X_index_min, X_index_max, x_index_min, x_index_max) = extract_slices_range(split, next_read_index, Y_size, Z_size)
                    write_offset = split.split_header_size + x_index_min * split.split_y * split.split_z * split.bytes_per_voxel
                    with open(split_name, 'a+b') as split_img:
                        # get y,z range of the original image that to be written
                        y_index_min = int(split.split_pos[-3])
                        z_index_min = int(split.split_pos[-2])
                        y_index_max = y_index_min + split.split_y
                        z_index_max = z_index_min + split.split_z
                        # time to write to file
                        # cut the proper size from the original image and write them (np array) to each split file
                        seek_time, write_time, seek_number = write_array_to_file(data_in_range[y_index_min: y_index_max, z_index_min: z_index_max, X_index_min - from_x_index: X_index_max - from_x_index + 1]
                                            .flatten('F'), split_img, write_offset)

                        total_write_time += write_time
                        total_seek_time += seek_time
                        total_seek_number += seek_number
            # update next read range..
            next_read_index = (next_read_index[1] + 1, next_read_index[1] + voxels)
            #  last write, write no more than image size
            if next_read_index[1] >= original_img_voxels:
                next_read_index = (next_read_index[0], original_img_voxels - 1)
            # if write range is larger img size, we are done
            if next_read_index[0] >= original_img_voxels:
                break
            # clear
            del data_in_range

        # for benchmarking
        print "************************************************"
        print "Total time spent reading: ", total_read_time
        print "Total time spent writing: ", total_write_time
        print "Total time spent seeking: ", total_seek_time
        print "Total number of seeks:", total_seek_number
        print "************************************************"

    def reconstruct_img(self, legend, merge_func, mem=None):
        """

        Keyword arguments:
        legend          : a legend containing the location of the blocks or slices located within the local filesystem to use for reconstruction
        merge_func      : the method in which the merging should be performed 0 or block_block for reading blocks and writing blocks, 1 or block_slice for
                          reading blocks and writing slices (i.e. cluster reads), and 2 or slice_slice for reading slices and writing slices
        mem             : the amount of available memory in bytes 
        """

        with open(self.filepath, self.file_access()) as reconstructed:

            if self.proxy is None:
                self.header.write_to(reconstructed)

            m_type = Merge[merge_func]

            self.merge_types[m_type](reconstructed, legend, mem)

        if self.proxy is None:
            self.proxy = self.load_image(self.filepath)

    # TODO:make it work with HDFS
    def clustered_read(self, reconstructed, legend, mem):
        """ Reconstruct an image given a set of splits and amount of available memory such that it can load subset of splits into memory for faster processing.
            Assumes all blocks are of the same dimensions. 
        Keyword arguments:
        reconstructed          : the fileobject pointing to the to-be reconstructed image
        legend                 : legend containing the URIs of the splits. Splits should be ordered in the way they should be written (i.e. along first dimension, 
                                 then second, then third) for best performance
        mem                    : Amount of available memory in bytes. If mem is None, it will only read one split at a time
        NOTE: currently only supports nifti blocks as it uses 'bitpix' to determine number of bytes per voxel. Element is specific to nifti headers
        """

        rec_dims = self.header.get_data_shape()

        y_size = rec_dims[0]
        z_size = rec_dims[1]
        x_size = rec_dims[2]

        bytes_per_voxel = self.header['bitpix'] / 8


        total_read = 0
        total_seek = 0
        total_num_seeks = 0
        total_write = 0

        # if a mem is inputted as 0, proceed with naive implementation (same as not inputting a value for mem)
        mem = None if mem == 0 else mem

        splits = sort_split_names(legend)
        #eof = splits[-1].strip()
        remaining_mem = mem
        data_dict = {}

        unread_split = None
        
        start_index = 0
        end_index = 0

        while start_index < len(splits):

            t = time()
            
            if mem is not None:
                end_index = self.initialize_dict(data_dict, remaining_mem, splits, start_index, bytes_per_voxel, y_size, z_size, x_size)
            else:
                end_index += 1

            print "End index: ", end_index 
            self.insert_elems(data_dict, splits, start_index, end_index, bytes_per_voxel, y_size, z_size, x_size)
            read_time = time() - t

            start_index = end_index


            seek_time, num_seeks, write_time = write_to_file(data_dict, reconstructed, bytes_per_voxel)

            total_read += read_time
            total_seek += seek_time
            total_num_seeks += num_seeks
            total_write += write_time

            remaining_mem = mem

        print "Total time spent reading: ", total_read
        print "Total time spent seeking: ", total_seek
        print "Total number of seeks: ", total_num_seeks
        print "Total time spent writing: ", total_write

    def insert_elems(self, data_dict, splits, start_index, end_index, bytes_per_voxel, y_size, z_size, x_size):
        """
        Insert elements into pre-initialized dictionary. If naive implementation of clustered reads, create dictionary with elements.
        
        Keyword arguments:

        data_dict       - pre-initialized or empty (if naive) dictionary to store key-value pairs representing seek position and value to be written, respectively
        splits          - list of split filenames
        start_index     - Start position in splits for instance of clustered read
        end_index       - End position in splits for instance of clustered reads
        bytes_per_voxel - Amount of bytes in a voxel in the reconstructed image
        y_size          - first dimension's array size in reconstructed image
        z_size          - second dimensions's array size in reconstructed image
        x_size          - third dimensions's array size in reconstructed image

        """
    
        write_type = None
        start_split = Split(splits[start_index].strip())
        start_pos = pos_to_int_tuple(start_split.split_pos)

        end_split = Split(splits[end_index - 1].strip())
        split_pos = pos_to_int_tuple(end_split.split_pos) 
        end_pos = (split_pos[0] + end_split.split_y, split_pos[1] + end_split.split_z, split_pos[2] + end_split.split_x)

        # determine write configuration
        if start_pos[0] == 0 and start_pos[1] == 0 and end_pos[0] == y_size and end_pos[1] == z_size:
            write_type = ClusteredWrites.SLICES
        
        elif start_pos[0] == 0 and end_pos[0] == y_size:
            write_type = ClusteredWrites.COMPLETE

        else:  
            write_type = ClusteredWrites.INCOMPLETE

        for i in range(start_index, end_index):

            split_im = Split(splits[i].strip())
            split_pos = pos_to_int_tuple(split_im.split_pos)
            idx_start = 0


            # split is a complete slice
            # WARNING: Untested
            if split_im.split_y  == y_size and split_im.split_z == z_size:
                data_len = split_im.split_y * split_im.split_z * split_im.split_x

                key = self.header.single_vox_offset + bytes_per_voxel * (
                        start_pos[0] + start_pos[1] * y_size + start_pos[2] * y_size * z_size)

                if write_type == ClusteredWrites.SLICES:

                    idx_start = (split_pos[0] - start_pos[0]) + (split_pos[1] - start_pos[1]) * y_size + (
                        split_pos[2] - start_pos[2]) * y_size * z_size
                    idx_end =  idx_start + data_len
        
                    split_data = split_im.split_proxy.get_data()
              
                    try: 
                        data_dict[k][idx_start:idx_end] = split_data.flatten('F')  
                    # is a naive read        
                    except:
                        data_dict[k] = split_data.flatten('F')                
                else:
                    print "ERROR: writing less than a slice at a time, but split is a slice"
                    sys.exit(1)

            # split is a complete row
            # WARNING: Untested
            elif split_im.split_y == y_size and split_im.split_z < z_size:
                data_len = split_im.split_y * split_im.split_z

                for i in xrange(split_im.split_x):
                
                    if write_type == ClusteredWrites.SLICES:
                        key = self.header.single_vox_offset + bytes_per_voxel * (
                            start_pos[0] + start_pos[1] * y_size + (start_pos[2]) * y_size * z_size)

                        idx_start = (split_pos[0] - start_pos[0]) + (split_pos[1] - start_pos[1]) * y_size + (
                            split_pos[2] + i - start_pos[2]) * y_size * z_size 
                        idx_end =  idx_start + data_len 

                    elif write_type == ClusteredWrites.COMPLETE:
                        dict_key = self.header.single_vox_offset + bytes_per_voxel * (
                            start_pos[0] + (start_pos[1] * y_size) + (start_pos[2] + i) * y_size * z_size)
                        
                        idx_start = (split_pos[0] - start_pos[0]) + (split_pos[1] - start_pos[1]) * y_size 

                    else:  
                        print "ERROR: Reading complete rows, but writing incomplete rows"

                    idx_end = idx_start + data_len
                    split_data = split_im.split_proxy.get_data()
            
                    try:
                        data_dict[k][idx_start:idx_end] = split_data[:, :, i].flatten('F')
                    # is a naive read
                    except:
                        data_dict[k] = split_data[:, :, i].flatten('F')
                
                
            # split is an incomplete row
            else:
                data_len = split_im.split_y

                idx_start = 0
                idx_end = 0
                
                for i in xrange(split_im.split_x):
                    for j in xrange(split_im.split_z):
                        
                        if write_type == ClusteredWrites.SLICES:
                            dict_key = self.header.single_vox_offset + bytes_per_voxel * (
                                start_pos[0] + (start_pos[1] * y_size) + (start_pos[2] * y_size * z_size))

                            idx_start = (split_pos[0] - start_pos[0]) + (split_pos[1] + j - start_pos[1]) * y_size + (
                                split_pos[2] + i - start_pos[2]) * y_size * z_size
                            
                        elif write_type == ClusteredWrites.COMPLETE:
                            dict_key = self.header.single_vox_offset + bytes_per_voxel * (
                                start_pos[0] + (start_pos[1] * y_size) + (start_pos[2] + i) * y_size * z_size)
                        
                            idx_start = (split_pos[0] - start_pos[0]) + (split_pos[1] + j - start_pos[1]) * y_size   

                        else:
                            dict_key = self.header.single_vox_offset + bytes_per_voxel * (
                                start_pos[0] + (start_pos[1] + j) * y_size + (start_pos[2] + i) * y_size * z_size)

                            idx_start = split_pos[0] - start_pos[0]
                            
                        idx_end = idx_start + data_len

                        split_data = split_im.split_proxy.get_data()

                        try:
                            data_dict[dict_key][idx_start:idx_end] = split_data[:, j, i]
                        except:
                            # Naive reading
                            data_dict[dict_key] = split_data[:, j, i]


    def initialize_dict(self, data_dict, remaining_mem, splits, start_index, bytes_per_voxel, y_size, z_size, x_size):
        """
        Pre-initialize dictionary for inserting data

        Keyword arguments:

        data_dict       - pre-initialized or empty (if naive) dictionary to store key-value pairs representing seek position and value to be written, respectively
        remaining_mem   - remaining available memory in bytes
        splits          - list of split filenames (sorted)
        start_index     - Start position in splits for instance of clustered read
        bytes_per_voxel - number of bytes for a voxel in the reconstructed image
        y_size          - first dimension of reconstructed image's array size
        z_size          - second dimension of reconstructed image's array size
        x_size          - third dimension of reconstructed image's array size

        Returns: update end index of read

        """

        split_name = splits[start_index].strip()
        
        start_im = Split(split_name)
        start_pos = pos_to_int_tuple(start_im.split_pos)
            
        remaining_mem -= start_im.split_bytes

        if remaining_mem < 0:
            print "ERROR: insufficient memory provided"
            sys.exit(1)

        split_positions = []
        split_positions.append(start_pos)

        end_idx = 0

        for i in range(start_index + 1, len(splits)):

            split_name = splits[i].strip()
            split_im = Split(split_name)
            split_pos = pos_to_int_tuple(split_im.split_pos)

            remaining_mem -= split_im.split_bytes

            if remaining_mem >= 0:
                split_positions.append(split_pos)

            end_idx = i
            if remaining_mem <= 0:
                break                

        # last header read does not fit in memory. Load previous header
        if remaining_mem < 0:

            split_im = Split(splits[i-1].strip())
            split_pos = pos_to_int_tuple(split_im.split_pos)
            end_idx -= 1
        
        end_pos = (split_pos[0] + split_im.split_y, split_pos[1] + split_im.split_z, split_pos[2] + split_im.split_x)
        split_im, end_idx, end_pos = adjust_end_read(splits, start_pos, split_pos, end_pos, start_index, end_idx, split_positions, y_size, z_size)
        
        print "Writing from position {0} to {1}".format(start_pos, end_pos)
        # case 1 - complete slices
        if start_pos[0] == 0 and start_pos[1] == 0 and end_pos[0] == y_size and end_pos[1] == z_size:
            print "Allocating complete slices"
            
            data_len = (end_pos[0] - start_pos[0]) * (end_pos[1] - start_pos[1]) * (end_pos[2] - start_pos[2])

            print "Initializing to data length: ", data_len

            key = self.header.single_vox_offset + bytes_per_voxel * (
                start_pos[0] + (start_pos[1] * y_size) + (start_pos[2]  * y_size * z_size))

            data_dict[key] = np.empty(data_len, dtype=self.dtype)


        # case 2 - complete rows
        elif start_pos[0] == 0 and end_pos[0] == y_size:
            print "Allocating complete rows"
            
            data_len = (end_pos[0] - start_pos[0]) * (end_pos[1] - start_pos[1])

            print "Initializing to data length: ", data_len

            for j in xrange(end_pos[2] - start_pos[2]):
                key = self.header.single_vox_offset + bytes_per_voxel * (
                    start_pos[0] + (start_pos[1] * y_size) + ((start_pos[2] + j) * y_size * z_size))

                data_dict[key] = np.empty(data_len, dtype = self.dtype)

        # case 3 - incomplete row + complete row
        elif end_pos[1] - split_shape[1] >= start_pos[1] + start_shape[1] and ((start_pos[0] > 0) or (end_pos[0] < y_size)):

            # write complete row(s) 
            if start_pos[0] == 0:

                print "Allocating complete rows"
                
                data_len = y_size * (split_pos[1] - start_pos[1])

                print "Initializing to data length: ", data_len

                for j in xrange(start_shape[2]):
                    key = self.header.single_vox_offset + bytes_per_voxel * (
                        (start_pos[1] * y_size) + ((start_pos[2] + j) * y_size * z_size))

                    data_dict[key] = np.empty(data_len, dtype = self.dtype)
                    
                
            # write incomplete row
            else:
            
                print "Allocating incomplete row"

                data_len = y_size - start_pos[0]

                print "Initializing to data length: ", data_len

                for j in xrange(split_shape[2]):
                    for k in xrange(split_shape[1]):
                        key = self.header.single_vox_offset + bytes_per_voxel * (
                            start_pos[0] + ((start_pos[1] + k) * y_size) + ((start_pos[2] + j) * y_size * z_size))

                        data_dict[key] = np.empty(data_len, dtype = self.dtype)
    
        # case 4: one incomplete row 
        elif (start_pos[0] > 0 or end_pos[0] < y_size) and start_pos[1] == split_pos[1]:

            print "Allocating incomplete row"

            data_len = end_pos[0] - start_pos[0]

            print "Initializing to data length: ", data_len
            for j in xrange(start_shape[2]):
                for k in xrange(start_shape[1]):
                    key = self.header.single_vox_offset + bytes_per_voxel * (
                        start_pos[0] + ((start_pos[1] + k) * y_size) + ((start_pos[2] + j) * y_size * z_size))

                    data_dict[key] = np.empty(data_len, dtype = self.dtype)
        else:
            print "ERROR: case not currently supported ", start_pos, end_pos
            sys.exit(1)     
    
        return end_idx + 1                       
                    
                

    def multiple_reads(self, reconstructed, legend, mem):
        """ Reconstruct an image given a set of splits and amount of available memory.
        multiple_reads: load splits servel times to read a complete slice
        Currently it can work on random shape of splits and in unsorted order
        :param reconstructed: the fileobject pointing to the to-be reconstructed image
        :param legend: containing the URIs of the splits.
        :param mem: bytes to be written into the file
        """
        Y_size, Z_size, X_size = self.header.get_data_shape()
        bytes_per_voxel = self.header['bitpix'] / 8
        header_offset = self.header.single_vox_offset
        reconstructed_img_voxels = X_size * Y_size * Z_size

        # for benchmarking
        total_read_time = 0
        total_seek_time = 0
        total_write_time = 0
        total_seek_number = 0

        # get how many voxels per round
        voxels = mem / bytes_per_voxel
        next_write_index = (0, voxels - 1)

        # read the headers of all the splits
        # to filter the splits out of the write range
        print "Get split offsets..."
        split_offsets = get_offsets_of_all_splits(legend, Y_size, Z_size)

        print "Sort split names..."
        sorted_split_name_list = sort_split_names(legend)

        # Core loop
        while True:
            # exclude header size
            next_write_offsets = (next_write_index[0] * bytes_per_voxel, next_write_index[1] * bytes_per_voxel + 1)
            print("From {} to {}...".format(next_write_offsets[0], next_write_offsets[1]))
            # the real write offset(with header size)
            write_offset = header_offset + next_write_offsets[0]
            # pre-assign a numpy array to store the data that going to be written consistently
            data_array = np.empty(shape=next_write_index[1] + 1 - next_write_index[0], dtype=self.header.get_data_dtype())

            # for benchmarking
            seek_number_one_r = 0
            read_time_one_r = 0
            start_read_time = time()

            # Flag - check if already found the first split in range,
            # if not, keep finding,
            # if yes, break at the next "not found", as the split names are sorted
            found_first_split_in_range = False
            # iterate all the splits
            for split_name in sorted_split_name_list:
                # check if split is in the write range, if there is at least one voxel in the range, then read the split.
                in_range = check_in_range(next_write_offsets, split_offsets[split_name])
                if in_range:
                    found_first_split_in_range = True
                    extract_rows(Split(split_name), data_array, next_write_index, Y_size, Z_size)
                    seek_number_one_r += 1
                elif not found_first_split_in_range:
                    continue
                else:
                    # because splits are sorted
                    break

            # for benchmarking
            read_time_one_r += time() - start_read_time
            print "seek_number_one_r: ", seek_number_one_r
            print "read_time_one_r: ", read_time_one_r
            total_seek_number += seek_number_one_r
            total_read_time += read_time_one_r

            # It's time to write array into the file.
            seek_time_w, write_time, seek_number_w = write_array_to_file(data_array, reconstructed, write_offset)

            # for benchmarking
            total_seek_time += seek_time_w
            total_write_time += write_time
            total_seek_number += seek_number_w

            # clean and prepare next round
            del data_array

            next_write_index = (next_write_index[1] + 1, next_write_index[1] + voxels)

            #  last write, write no more than image size
            if next_write_index[1] >= reconstructed_img_voxels:
                next_write_index = (next_write_index[0], reconstructed_img_voxels - 1)

            # if write range is larger img size, we are done
            if next_write_index[0] >= reconstructed_img_voxels:
                break

        # for benchmarking
        print "************************************************"
        print "Total time spent reading: ", total_read_time
        print "Total time spent writing: ", total_write_time
        print "Total time spent seeking: ", total_seek_time
        print "Total number of seeks: ", total_seek_number
        print "************************************************"


    def load_image(self, filepath, in_hdfs=None):

        """Load image into nibabel
        Keyword arguments:
        filepath                        : The absolute, relative path, or HDFS URL of the image
                                          **Note: If in_hdfs parameter is not set and file is located in HDFS, it is necessary that the path provided is
                                                an HDFS URL or an absolute/relative path with the '_HDFSUTILS_FLAG_' flag as prefix, or else it will
                                                conclude that file is located in local filesystem.
        in_hdfs                         : boolean variable indicating if image is located in HDFS or local filesystem. By default is set to None.
                                          If not set (i.e. None), the function will attempt to determine where the file is located.   
        """

        if self.utils is None:
            in_hdfs = False
        elif in_hdfs is None:
            in_hdfs = self.utils.is_hdfs_uri(filepath)

        if in_hdfs:
            fh = None
            # gets hdfs path in the case an hdfs uri was provided
            filepath = self.utils.hdfs_path(filepath)

            with self.utils.client.read(filepath) as reader:
                stream = reader.read()
                if self.is_gzipped(filepath, stream[:2]):
                    fh = nib.FileHolder(fileobj=GzipFile(fileobj=BytesIO(stream)))
                else:
                    fh = nib.FileHolder(fileobj=BytesIO(stream))

                if is_nifti(filepath):
                    return nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})
                if is_minc(filepath):
                    return nib.Minc1Image.from_file_map({'header': fh, 'image': fh})
                else:
                    print('ERROR: currently unsupported file-format')
                    sys.exit(1)
        elif not os.path.isfile(filepath):
            logging.warn("File does not exist in HDFS nor in Local FS. Will only be able to reconstruct image...")
            return None

        # image is located in local filesystem
        try:
            return nib.load(filepath)
        except:
            print "ERROR: Unable to load image into nibabel"
            sys.exit(1)

    def file_access(self):

        if self.proxy is None:
            return "w+b"
        return "r+b"

def adjust_end_read(splits, start_pos, split_pos, end_pos, start_index, end_idx, split_positions, y_size, z_size):
    """
    Adjusts the end split should the read not be a complete slice, complete row, or incomplete row
    
    Keyword arguments
    splits          - list of split filenames
    start_pos       - the starting position of the first split in the reconstructed array
    split_pos       - the starting position of the last split in the reconstructed array
    end_pos         - the end position of the last split in the reconstructed array
    start_index     - the starting index of the first split read in splits
    end_idx         - the end index of the last split read in splits
    split_positions - a list of all the read split's positions
    y_size          - the first dimension of the reconstructed image's array size
    z_size          - the second dimension of the reconstructed image's array size

    Returns: the "correct" last split, its index in splits, and its end position
    """  
 
    adj_end_idx = end_idx
         
    # adjust end split it incomplete row spanning different slices/rows
    if start_pos[0] > 0  and (start_pos[2] < split_pos[2] or start_pos[1] < split_pos[1]):
        # get first row's last split
        curr_end_y = start_pos[1]

        for x in range(1, len(split_positions)):
            if split_positions[x][1] != curr_end_y:
                adj_end_idx = start_index + x - 1
                break

    # adjust end split if splits are on different slices and incomplete slices are to be written. 
    elif start_pos[2] < split_pos[2]:

        # complete slices
        if start_pos[0] == 0 and start_pos[1] == 0 and (end_pos[0] < y_size or end_pos[1] < z_size):
            # need to find last split read before slice change
            
            curr_end_x = split_pos[2]
            for x in range(-2, -len(split_positions) -1, -1):
                if split_positions[x][2] < curr_end_x:
                    adj_end_idx = start_index + len(split_positions) + x
                    break

        # complete rows
        elif start_pos[0] == 0 and start_pos[1] > 0:

            # get first slice's last split
            curr_end_x = start_pos[2]
    
            for x in range(1, len(split_positions)):
                if split_positions[x][2] > curr_end_x:
                    adj_end_idx = start_index + x - 1
                    break
     
    # adjust end split if splits start on the same slice but on different rows, and read splits contain and incomplete row and a complete row
    elif start_pos[2] == split_pos[2] and start_pos[1] < split_pos[1] and end_pos[0] != y_size:                    

        # get last split of second-to-last row 
        curr_end_y = split_pos[1]
        
        for x in range(-2, -len(split_positions) -1, -1):
            if split_positions[x][1] < curr_end_y:
                adj_end_idx = start_index + len(split_positions) + x
                break

    #load new end
    if adj_end_idx != end_idx:
        split_im = Split(splits[adj_end_idx].strip())
        split_pos = pos_to_int_tuple(split_im.split_pos)
        end_pos = (split_pos[0] + split_im.split_y, split_pos[1] + split_im.split_z, split_pos[2] + split_im.split_x)

    return Split(splits[adj_end_idx].strip()), adj_end_idx, end_pos

    


def generate_splits_name(y_size, z_size, x_size, Y_size, Z_size, X_size, out_dir, filename_prefix, extension):
    """
    generate all the splits' name based on the number of splits the user set
    """
    split_names = []
    for x in range(0, X_size, x_size):
        for z in range(0, Z_size, z_size):
            for y in range(0, Y_size, y_size):
                split_names.append(out_dir + '/' + filename_prefix + '_' + str(y) + "_" + str(z) + "_" + str(x) + "." + extension)
    return split_names

def generate_legend_file(split_names, legend_file_name, out_dir):
    """
    generate legend file for each all the splits
    """
    legend_file = '{0}/{1}'.format(out_dir, legend_file_name)
    with open(legend_file, 'a+') as f:
        for split_name in split_names:
            f.write('{0}\n'.format(split_name))
    return legend_file

def generate_headers_of_splits(split_names, y_size, z_size, x_size, dtype):
    """
    generate headers of each splits based on the shape and dtype 
    """
    header = generate_header(y_size, z_size, x_size, dtype)
    for split_name in split_names:
        with open(split_name, 'w+b') as f:
            header.write_to(f)

def index_to_voxel(index, Y_size, Z_size):
    """
    index to voxel, eg. 0 -> (0,0,0).
    """
    i = index % (Y_size )
    index = index / (Y_size)
    j = index % (Z_size )
    index = index / (Z_size )
    k = index
    return (i, j, k)

def extract_slices_range(split, next_read_index, Y_size, Z_size):
    """
    extract all the slices of each split that in the read range.
    X_index: index that in original image's coordinate system
    x_index: index that in the split's coordinate system
    """
    indexes = []
    x_index_min = -1
    read_start, read_end = next_read_index
    for i in range(0, split.split_x):
        index = int(split.split_pos[-3]) + (int(split.split_pos[-2])) * Y_size + (int(split.split_pos[-1]) + i) * Y_size * Z_size
        # if split's one row is in the write range.
        if index >= read_start and index <= read_end:
            if len(indexes) == 0:
                x_index_min = i
            indexes.append(index)
        else:
            continue

    X_index_min = index_to_voxel(min(indexes), Y_size, Z_size)[2]
    X_index_max = index_to_voxel(max(indexes), Y_size, Z_size)[2]
    x_index_max = x_index_min + (X_index_max - X_index_min)

    return (X_index_min, X_index_max, x_index_min, x_index_max)

class Split:
    """
    It contains all the info of one split
    """
    def __init__(self, split_name):
        self.split_name = split_name
        self._get_info_from(split_name)

    def _get_info_from(self, split_name):
        self.split_pos = split_ext(split_name)[0].split('_')
        self.split_proxy = nib.load(split_name)
        self.split_header_size = self.split_proxy.header.single_vox_offset
        self.bytes_per_voxel = self.split_proxy.header['bitpix'] / 8
        (self.split_y, self.split_z, self.split_x) = self.split_proxy.header.get_data_shape()
        self.split_bytes = self.bytes_per_voxel * (self.split_y * self.split_x * self.split_z)

def sort_split_names(legend):
    """
    sort all the split names read from legend file
    output a sorted name list
    """
    split_position_list = []
    sorted_split_names_list = []
    split_name = ""
    with open(legend, "r") as f:
        for split_name in f:
            split_name = split_name.strip()
            split = Split(split_name)
            split_position_list.append((int(split.split_pos[-3]), (int(split.split_pos[-2])),  (int(split.split_pos[-1]))))

    # sort the last element first in the tuple
    split_position_list = sorted(split_position_list, key=lambda t: t[::-1])
    for position in split_position_list:
        sorted_split_names_list.append(regenerate_split_name_from_position(split_name, position))
    return sorted_split_names_list

def regenerate_split_name_from_position(split_name, position):
    filename_prefix = split_name.strip().split('/')[-1].split('_')[0]
    filename_ext = split_name.strip().split(".", 1)[1]
    blocks_dir = split_name.strip().rsplit('/', 1)[0]
    split_name = blocks_dir + '/' + filename_prefix + "_" + str(position[0]) + "_" + str(position[1]) + "_" + str(position[2]) + "." + filename_ext
    return  split_name

def extract_rows(split, data_list, write_index, Y_size, Z_size):
    """
    extract_all the rows that in the write range, and write the data to a numpy array
    """
    write_start, write_end = write_index
    split_data = split.split_proxy.get_data()

    for i in range(0, split.split_x):
        for j in range(0, split.split_z):

            index_start = int(split.split_pos[-3]) + (int(split.split_pos[-2]) + j) * Y_size + (int(split.split_pos[-1]) + i) * Y_size * Z_size
            index_end = index_start + split.split_y

            # if split's one row is in the write range.
            if index_start >= write_start and index_end <= write_end:
                data = split_data[..., j, i]
                data_list[index_start - write_start: index_end - write_start] = data

            # if split's one row's start index is in the write range, but end index is outside of write range.
            elif index_start <= write_end <= index_end:
                data = split_data[: (write_end - index_start + 1), j, i]
                data_list[index_start - write_start: ] = data

            # if split's one row's end index is in the write range, but start index is outside of write range.
            elif index_start <= write_start <= index_end:
                data = split_data[write_start - index_start :, j, i]
                data_list[: index_end - write_start] = data

            # if not in the write range
            else:
                continue

def get_offsets_of_all_splits(legend, Y_size, Z_size):
    """
    get writing offsets of all splits, add them to a dictionary
    key-> split_name
    value-> a writing offsets list
    """
    split_offsets = {}
    with open(legend, "r") as f:
        for split_name in f:
            split_name = split_name.strip()
            split = Split(split_name)
            offset_list = get_offsets_of_split(split, Y_size, Z_size)
            split_offsets[split.split_name] = offset_list
    return split_offsets

def get_offsets_of_split(split, Y_size, Z_size):
    """
    get all the writing offset in one split
    """
    offset_list = []
    for i in range(0, split.split_x):
        for j in range(0, split.split_z):
            # calculate the offset (in bytes) of each tile, add all the tiles in to data_dict that in the write range.
            write_offset = split.bytes_per_voxel * (int(split.split_pos[-3]) + (int(split.split_pos[-2]) + j) * Y_size +
                                                                              (int(split.split_pos[-1]) + i) * Y_size * Z_size)
            offset_list.append(write_offset)
    return offset_list

def check_in_range(next_offsets, offset_list):
    """
    check if at least one voxel in the split in the write range
    """
    for offset in offset_list:
        if offset >= next_offsets[0] and offset <= next_offsets[1]:
            return True
    return False

def write_array_to_file(data_array, to_file, write_offset):
    """
    :param data_array: consists of consistent data that to bo written to the file
    :param reconstructed: reconstructed image file to be written 
    :param write_offset: file offset to be written
    :return: benchmarking params
    """
    seek_time = 0
    write_time = 0
    seek_number = 0

    seek_start = time()
    to_file.seek(write_offset, 0)
    seek_number += 1
    seek_time += time() - seek_start

    write_start = time()
    to_file.write(data_array.tobytes('F'))
    write_time += time() - write_start

    return seek_time, write_time, seek_number


def write_to_file(data_dict, reconstructed, bytes_per_voxel):
    """Write dictionary data to file
    Keyword arguments:
    data_dict                           : dictionary containing contiguous data chunks as key-value pairs (key is position in reconstructed in bytes)
    reconstructed                       : file object pointing to the to-be reconstructed image
    bytes_per_voxel                     : number of bytes contained in a voxel in the reconstructed image
    """

    seek_time = 0
    write_time = 0
    num_seeks = 0

    print "Writing data"
    for k in sorted(data_dict.keys()):
        
                
        seek_start = time()
        reconstructed.seek(k, 0)
        seek_time += time() - seek_start
    
        write_start = time()
        reconstructed.write(data_dict[k].tobytes('F'))
        write_time += time() - write_start

        num_seeks += 1
        # can remove from dictionary
        del data_dict[k]

    return seek_time,num_seeks, write_time


def generate_header(first_dim, second_dim, third_dim, dtype):
    # TODO: Fix header so that header information is accurate once data is filled
    # Assumes file data is 3D

    try:
        header = nib.Nifti1Header()
        header['dim'][0] = 3
        header['dim'][1] = first_dim
        header['dim'][2] = second_dim
        header['dim'][3] = third_dim
        header.set_data_dtype(dtype)

        return header

    except:
        print "ERROR: Unable to generate header. Please verify that the dimensions and datatype are valid."
        sys.exit(1)


def is_gzipped(filepath, buff=None):
    """Determine if image is gzipped
    Keyword arguments:
    filepath                        : the absolute or relative filepath to the image
    buffer                          : the bystream buffer. By default the value is None. If the image is located on HDFS,
                                      it is necessary to provide a buffer, otherwise, the program will terminate.
    """
    mime = magic.Magic(mime=True)
    try:
        if buff is None:
            if 'gzip' in mime.from_file(filepath):
                return True
            return False
        else:
            if 'gzip' in mime.from_buffer(buff):
                return True
            return False
    except:
        print('ERROR: an error occured while attempting to determine if file is gzipped')
        sys.exit(1)


def is_nifti(fp):
    ext = split_ext(fp)[1]
    if '.nii' in ext:
        return True
    return False


def is_minc(fp):
    ext = split_ext(fp)[1]
    if '.mnc' in ext:
        return True
    return False


def split_ext(filepath):
    # assumes that if '.mnc' of '.nii' not in gzipped file extension, all extensions have been removed
    root, ext = os.path.splitext(filepath)
    ext_1 = ext.lower()
    if '.gz' in ext_1:
        root, ext = os.path.splitext(root)
        ext_2 = ext.lower()
        if '.mnc' in ext_2 or '.nii' in ext_2:
            return root, "".join((ext_1, ext_2))
        else:
            return "".join((root, ext)), ext_1

    return root, ext_1

def pos_to_int_tuple(pos):
    return (int(pos[-3]), int(pos[-2]), int(pos[-1]))

get_bytes_per_voxel = {'uint8': np.dtype('uint8').itemsize,
                       'uint16': np.dtype('uint16').itemsize,
                       'uint32': np.dtype('uint32').itemsize,
                       'ushort': np.dtype('ushort').itemsize,
                       'int8': np.dtype('int8').itemsize,
                       'int16': np.dtype('int16').itemsize,
                       'int32': np.dtype('int32').itemsize,
                       'int64': np.dtype('int64').itemsize,
                       'float32': np.dtype('float32').itemsize,
                       'float64': np.dtype('float64').itemsize
                       }


def generate_zero_nifti(output_filename, first_dim, second_dim, third_dim, dtype, mem=None):
    """ Function that generates a zero-filled NIFTI-1 image.
    Keyword arguments:
    output_filename               : the filename of the resulting output file
    first_dim                     : the first dimension's (x?) size
    second_dim                    : the second dimension's (y?) size
    third_dim                     : the third dimension's (z?) size
    dtye                          : the Numpy datatype of the resulting image
    mem                           : the amount of available memory. By default it will only write one slice of zero-valued voxels at a time
    """
    with open(output_filename, 'wb') as output:
        generate_header(output_fo, first_dim, second_dim, third_dim, dtype)

        bytes_per_voxel = header['bitpix'] / 8

        slice_bytes = bytes_per_voxel * (first_dim * second_dim)

        slice_third_dim = 1
        slice_remainder = 0

        if mem is not None:
            slice_third_dim = mem / slice_bytes
            slice_remainder = (bytes_per_voxel * (first_dim * second_dim * third_dim)) % (slice_bytes * slice_third_dim)
            print slice_remainder

        slice_arr = np.zeros((first_dim, second_dim, slice_third_dim), dtype=dtype)

        print "Filling image..."
        for x in range(0, third_dim / slice_third_dim):
            output.write(slice_arr.tobytes('F'))

        if slice_remainder != 0:
            output.write(np.zeros((first_dim, second_dim, slice_third_dim), dtype=dtype).tobytes('F'))


# TODO:Not yet fully implemented
class Legend:
    def __init__(self, filename, is_img):
        self.nib = is_img
        self.pos = 0

        # Not even sure that this can or should be done
        try:
            if not is_img:
                self.contents = open(filename, 'r')
            else:
                self.contents = nib.load(filename).get_data().flatten()
        except:
            print "ERROR: Legend is not a text file or a nibabel-supported image"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.nib:
            self.contents.close()

    def get_next_block():
        if not self.nib:
            return self.contents.readline()
        else:
            self.pos += 1
            return str(int(self.contents[self.pos - 1])).zfill(4)

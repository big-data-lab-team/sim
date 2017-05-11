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


class Merge(Enum):
    block_block = 0
    block_slice = 1
    slice_slice = 2

class ImageUtils:
    """ Helper utility class for performing operations on images"""

    def __init__(self, filepath, first_dim = None, second_dim = None, third_dim = None, dtype = None, utils = None):
        #TODO: Review. not sure about these instance variables...    
        
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

        self.affine = self.header.get_best_affine()
        self.header_size = self.header.single_vox_offset

    
        self.merge_types = {Merge.block_block : self.reconstruct_block_block,
                            Merge.block_slice : self.reconstruct_block_slice,
                            Merge.slice_slice : self.reconstruct_slice_slice
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
                raise AttributeError( "Cannot split an image that has not yet been created.")
        except AttributeError as aerr:
            print 'AttributeError: ', aerr 
            sys.exit(1)

        num_x_iters = int(ceil(self.proxy.dataobj.shape[2]/third_dim))
        num_z_iters = int(ceil(self.proxy.dataobj.shape[1]/second_dim))
        num_y_iters = int(ceil(self.proxy.dataobj.shape[0]/first_dim))
        

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

                    x_start = x*third_dim
                    x_end = (x+1)*third_dim

                    z_start = z*second_dim
                    z_end = (z+1)*second_dim

                    y_start = y*first_dim
                    y_end = (y+1)*first_dim

                    split_array = self.proxy.dataobj[y_start:y_end, z_start:z_end, x_start:x_end]
                    split_image = nib.Nifti1Image(split_array, self.affine)
       
                    imagepath = None
                    
                    #TODO: fix this so that position saved in image and not in filename
                    #if the remaining number of voxels does not match the requested number of voxels, save the image with
                    #with the given filename prefix and the suffix: _<x starting coordinate>_<y starting coordinate>_<z starting coordinate>__rem-<x lenght>-<y-length>-<z length>
                    if is_rem_x or is_rem_y or is_rem_z:
                        imagepath = '{0}/{1}_{2}_{3}_{4}__rem-{5}-{6}-{7}.nii.gz'.format(local_dir, filename_prefix, y_start, z_start, x_start, y_length, z_length, x_length) 
                    else:
                        imagepath = '{0}/{1}_{2}_{3}_{4}.nii.gz'.format(local_dir, filename_prefix, y_start, z_start, x_start)
                    
                    nib.save(split_image, imagepath)

                    if copy_to_hdfs:
                        self.utils.copy_to_hdfs(imagepath, ovrwrt=True, save_path_to_file=True)
                    else:
                        with open('{0}/legend.txt'.format(local_dir), 'a+') as im_legend:
                            im_legend.write('{0}\n'.format(imagepath))

                    is_rem_z = False
        
            is_rem_y = False

    def reconstruct_img(self, legend, merge_func, mem = None):
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

            if m_type == Merge.block_block:
                self.merge_types[m_type](reconstructed, legend)
            else:
                self.merge_types[m_type](reconstructed, legend, mem)

        if self.proxy is None:
            self.proxy = self.load_image(self.filepath)


    #TODO: Fix function so that it will work in HDFS
    def reconstruct_block_block(self, reconstructed, legend):
        """ Reconstructs and image given a set of blocks. Filenames of blocks must contain their positioning in 3d in the reconstructed image
        (will work for slices too as long as filenames are configured properly)

        Keyword arguments:
        reconstructed                 : the fileobject pointing to the to-be reconstructed image
        legend                        : a legend containing the location of the blocks located within the local filesystem to use for reconstruction
                                        ex. path/to/block_x_y_z.ext or /path/to/block_x_y_z.ext

        NOTE: currently only supports nifti blocks as it uses 'bitpix' to determine number of bytes per voxel. Element is specific to nifti headers
        """

        bytes_per_voxel = self.header['bitpix']/8 
        rec_dims = self.header.get_data_shape()
     
        y_size = rec_dims[0]
        z_size = rec_dims[1]
        x_size = rec_dims[2]

        with open(legend, 'r')  as legend:
            prev_b_end = (0, 0, 0)
            for block_name in legend:

                block_name = block_name.strip()
                
                print "Writing block: {0}".format(block_name)
                
                block = nib.load(block_name)
            

                shape = block.header.get_data_shape()
                shape_y = shape[0]
                shape_z = shape[1]
                shape_x = shape[2]

                block_pos = split_ext(block_name)[0].split('_')

                block_y = int(block_pos[-3])
                block_z = int(block_pos[-2])
                block_x = int(block_pos[-1])
                block_data = block.get_data(caching='unchanged')
                
                t=time()
                max_seek = 0
                max_write = 0
                avg_seek = 0
                avg_write = 0
                min_seek = 1000
                min_write = 1000
                std_seek = 0
                std_write = 0

                seek_times = []
                write_times = []

                step_x = 1
                step_z = 1

                start_x = 0
                start_z = 0

                end_x = shape_x
                end_z = shape_z
                
                #print "Write starting at position: ({0}, {1}, {2})".format(block_y, block_z + start_z , block_x + start_x)

                for i in range(start_x, end_x, step_x):
                    for j in range(start_z, end_z, step_z):
                        seek_start = time()
                        reconstructed.seek(self.header_size + bytes_per_voxel*(block_y + (block_z + j)*y_size +(block_x + i)*y_size*z_size), 0)
                        seek_end = time() - seek_start
                        write_start = time()
                        reconstructed.write(block_data[:,j, i].tobytes())
                        write_end = time() - write_start

                        avg_seek += seek_end
                        avg_write += write_end 

                        seek_times.append(seek_end)
                        write_times.append(write_end)

                        if seek_end > max_seek:
                            max_seek = seek_end
                        if write_end > max_write:
                            max_write = write_end
                        if seek_end < min_seek:
                            min_seek = seek_end
                        if write_end < max_write:
                            min_write = write_end


                #print time()-t
                
                #avg_seek = avg_seek / (shape_x * shape_z)
                #avg_write = avg_write / (shape_x * shape_z)
                
                #print "Maximum seek time: {0}\t Maximum write time: {1}".format(max_seek, max_write)
                #print "Minimum seek time: {0}\t Minimum write time: {1}".format(min_seek, min_write)
                #print "Average seek time: {0}\t Average write time: {1}".format(avg_seek, avg_write)
                #print "Std seek: {0}\t Std write: {1}".format(np.std(seek_times), np.std(write_times))

                #if end_x == -1:
                #    end_x = 0
                #if end_z == -1:
                #    end_z = 0
                #prev_b_end = (block_y + shape[0], block_z + end_z, block_x + end_x)
                #print "End position of write: {0}".format(prev_b_end)

    def reconstruct_block_slice(self, reconstructed, legend, mem):
        """ Reconstruct an image given a set of blocks and amount of available memory such that it can load subset of blocks into memory for faster processing.
            Assumes all blocks are of the same dimensions. 

        Keyword arguments:
        reconstructed          : the fileobject pointing to the to-be reconstructed image
        legend                 : legend containing the URIs of the blocks. Blocks should be ordered in the way they should be written (i.e. along first dimension, 
                                 then second, then third)
        mem                    : Amount of available memory in bytes

        NOTE: currently only supports nifti blocks as it uses 'bitpix' to determine number of bytes per voxel. Element is specific to nifti headers
        """
  
        rec_dims = self.header.get_data_shape()

        y_size = rec_dims[0]
        z_size = rec_dims[1]
        x_size = rec_dims[2]

        bytes_per_voxel = self.header['bitpix']/8

        print y_size, z_size, x_size

        with open(legend, "r") as legend:

            remaining_mem = mem
            data = None
            start_x = 0
            start_z = 0
            start_y = 0

            prev_block_start = (0,0,0) 
            for block_name in legend:
                
                block_name = block_name.strip()
                print "Reading block {0}".format(block_name)
                block_pos = split_ext(block_name)[0].split('_')

                #we will start our (partial) slice at these block coordinates
                if remaining_mem == mem:
                    start_x = int(block_pos[-1])
                    start_z = int(block_pos[-2])
                    start_y = int(block_pos[-3])
                    
                block = nib.load(block_name)
                block_shape = block.header.get_data_shape()
                bytes_per_voxel_block = block.header['bitpix']/8
                block_bytes = block.header.single_vox_offset + bytes_per_voxel_block*(block_shape[0]*block_shape[1]*block_shape[2])
                
                if data is None:
                    data = block.get_data()
                else:
                    try:

                        #concatenate data on the right axis
                        if prev_block_start[1] == block_pos[-2] and prev_block_start[2] == block_pos[-1]:
                            data = np.concatenate((data,block.get_data()))
                        elif prev_block_start[1] != block_pos[-2] and prev_block_start[2] == block_pos[-1]:
                            data = np.concatenate((data,block.get_data()), 1)
                        elif prev_block_start[2] != block_pos[-1] and prev_block_start[1] == block_pos[-2]:
                            data = np.concatenate((data,block.get_data()), 2)
                        else:
                            raise Exception('Unable to concatenate data')
                    except Exception as e:
                        print 'ERROR: ', e
                        sys.exit(1)
                        
                remaining_mem = remaining_mem - (block_bytes)

                #cannot load another block into memory, must write slice
                if remaining_mem / block_bytes < 1:
                    print "Remaining memory:{0}".format(remaining_mem)

                    end_x = int(block_pos[-1]) + block_shape[2]
                    end_z = int(block_pos[-2]) + block_shape[1]
                    end_y = int(block_pos[-3]) + block_shape[0]

                    print "Writing data starting at ({0}, {1}, {2}) and ending at ({3}, {4}, {5})".format(start_y, start_z, start_x, end_y, end_z, end_x)
                    for i in range(0, block_shape[2]):
                        seek_start = time()
                        reconstructed.seek(self.header_size + bytes_per_voxel*(start_y + (start_z)*y_size +(start_x + i)*y_size*z_size), 0)
                        print "Seek time for slice of  depth {0}: {1}".format(start_x + i, time()-seek_start)
                        write_start = time()
                        reconstructed.write(data[:,:,i].tobytes('F'))
                        print "Write time for slice of  depth {0}: {1}".format(start_x + i, time()-write_start)
                    

                    remaining_mem = mem
                    data = None

                prev_block_start = (block_pos[-3], block_pos[-2], block_pos[-1])


    #TODO: Fix function so that it will work in HDFS
    def reconstruct_slice_slice(self, reconstructed, legend, mem=None):
        """ Reconstructs and image given a set of slices.

        Keyword arguments:
        reconstructed                   : the fileobject pointing to the to-be reconstructed image
        legend                          : a legend containing the location of the blocks to use for reconstruction.
        mem                             : the amount of available memory in bytes in the system. If None is specified, only one slice will be loaded into memory at a time

        NOTE: currently only supports nifti slices as it uses 'bitpix' to determine number of bytes per voxel. Element is specific to nifti headers

        """

        reconstructed.seek(self.header_size, 0)

        with open(legend, "r") as legend:

            remaining_mem = mem
            data = None                

            for slice_name in legend:
                
                if slice_name == "" or slice_name is None:
                    continue                

                slice_name = slice_name.strip()
                print "Loading slice: {0}".format(slice_name)
                
                slice_img = self.load_image(slice_name)
                slice_shape = slice_img.header.get_data_shape()
                slice_bytes = slice_img.header.single_vox_offset + slice_img.header['bitpix']/8 * (slice_shape[0] * slice_shape[1] * slice_shape[2])


                if data is None:
                    data = slice_img.get_data()[...]
                else:
                    data = np.concatenate((data, slice_img.get_data()[...]), axis = 2)    
                
                if remaining_mem is not None:
                    remaining_mem = remaining_mem - slice_bytes

                if remaining_mem is None or remaining_mem / slice_bytes < 1:
                    write_start = time()
                    reconstructed.write(data.tobytes('F'))
                    print "Write time for slices: {0}".format(time()-write_start)
        
                    data = None
                    remaining_mem = mem


    def load_image(self, filepath, in_hdfs = None):
        
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
            #gets hdfs path in the case an hdfs uri was provided
            filepath = self.utils.hdfs_path(filepath)

            with self.utils.client.read(filepath) as reader:
                stream = reader.read()
                if self.is_gzipped(filepath, stream[:2]):
                    fh = nib.FileHolder(fileobj=GzipFile(fileobj=BytesIO(stream)))
                else:
                    fh = nib.FileHolder(fileobj=BytesIO(stream))
                
                if is_nifti(filepath):
                    return nib.Nifti1Image.from_file_map({'header':fh, 'image':fh})
                if is_minc(filepath):
                    return nib.Minc1Image.from_file_map({'header':fh, 'image':fh})
                else:
                    print('ERROR: currently unsupported file-format')
                    sys.exit(1)
        elif not os.path.isfile(filepath):
            logging.warn("File does not exist in HDFS nor in Local FS. Will only be able to reconstruct image...")
            return None

        #image is located in local filesystem
        try:
            return nib.load(filepath)
        except:
            print "ERROR: Unable to load image into nibabel"
            sys.exit(1)

    def file_access(self):
        
        if self.proxy is None:
            return "w+b"
        return "r+b"

def generate_header(first_dim, second_dim, third_dim, dtype):
    #TODO: Fix header so that header information is accurate once data is filled
    #Assumes file data is 3D

    try:
        header = nib.Nifti1Header()
        header['dim'][0] = 3
        header['dim'][1] = first_dim
        header['dim'][2] = second_dim
        header['dim'][3]= third_dim
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
    #assumes that if '.mnc' of '.nii' not in gzipped file extension, all extensions have been removed
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


get_bytes_per_voxel = { 'uint8' : np.dtype('uint8').itemsize,
                        'uint16' : np.dtype('uint16').itemsize,
                        'uint32' : np.dtype('uint32').itemsize,
                        'ushort' : np.dtype('ushort').itemsize,
                        'int8' : np.dtype('int8').itemsize,
                        'int16' : np.dtype('int16').itemsize,
                        'int32' : np.dtype('int32').itemsize,
                        'int64' : np.dtype('int64').itemsize,
                        'float32': np.dtype('float32').itemsize,
                        'float64' : np.dtype('float64').itemsize
}

                    

def generate_zero_nifti(output_filename, first_dim, second_dim, third_dim, dtype, mem = None):
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

        bytes_per_voxel = header['bitpix']/8
            
        slice_bytes = bytes_per_voxel * (first_dim * second_dim)

        slice_third_dim = 1
        slice_remainder = 0

        if mem is not None:
            slice_third_dim = mem / slice_bytes
            slice_remainder = (bytes_per_voxel * (first_dim * second_dim * third_dim)) % (slice_bytes * slice_third_dim)
            print slice_remainder
        
        slice_arr = np.zeros((first_dim,second_dim, slice_third_dim), dtype=dtype)
        
        print "Filling image..."           
        for x in range(0, third_dim/slice_third_dim):
            output.write(slice_arr.tobytes('F'))

        if slice_remainder != 0:
            output.write(np.zeros((first_dim, second_dim, slice_third_dim), dtype=dtype).tobytes('F'))



#TODO:Not yet fully implemented
class Legend:

    def __init__(self, filename, is_img):
        self.nib = is_img
        self.pos = 0

        #Not even sure that this can or should be done
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
             
        
                         

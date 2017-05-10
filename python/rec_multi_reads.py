#!/usr/bin/env python
import __future__
import nibabel as nib
import numpy as np
import time

# image shape: 3850 3025 3500
# file name format: bigbrain_3080_1815_1400.nii.gz


# info of header. Code should be modified before merging to imageutils.py
header_size = 352
bytes_per_voxel = 2

# TODO: Memory, hdfs, other file formats
def reconstruct_multi_reads(legend, img_shape, blocks_dir, out_image):
    """
    Reconstruct nii images from several blocks  
    Read blocks, write big slice
    
    :param legend: legend file of the image 
    :param img_shape: shape of the image
    :param blocks_dir: input blocks directory
    :param out_image: output file name
    :return: None
    """
    y_size = img_shape[0]
    z_size = img_shape[1]
    x_size = img_shape[2]

    # extract input block name from the first line of the legend file
    with open(legend, 'r')  as legend:
        filename_prefix = legend.readline().strip().split('/')[-1].split('_')[0]
        filename_ext = legend.readline().strip().split(".", 1)[1]
        print "filename_prefix: {0}, filename_ext: {1}".format(filename_prefix, filename_ext)


    with open(out_image, "ab+") as reconstructed:

        # n blocks each dimension, for now 25 blocks per slice in x dimension
        n = 5

        for l, x in enumerate(range(0, x_size, x_size // n)):
            np_arr_slice = None
            for z in range(0, z_size, z_size // n):
                np_arr_axis_y = None
                for y in range(0, y_size, y_size // n):
                    block_name = blocks_dir + '/' + filename_prefix + "_" + str(y) + "_" + str(z) + "_" + str(x) + "." + filename_ext
                    print "Loading block: {0} into memory".format(block_name)
                    block_data = nib.load(block_name).get_data(caching='unchanged')
                    # load y axis blocks -> big slice -> extend arr
                    if np_arr_axis_y is None:
                        np_arr_axis_y = block_data
                    else:
                        np_arr_axis_y = np.dstack((np_arr_axis_y, block_data))
                # save y axis blocks to a big slice (arr)
                if np_arr_slice is None:
                    np_arr_slice = np_arr_axis_y
                else:
                    np_arr_slice = np.hstack((np_arr_slice, np_arr_axis_y))

            # save the big slice (arr) into file: (1) seek (2) write
            one_slice_time = time.time()
            print "seeking to {}".format(header_size + bytes_per_voxel * (l * (x_size // n) * y_size * z_size))
            reconstructed.seek(header_size + bytes_per_voxel * (l * (x_size // n) * y_size * z_size), 0)
            print "writing bytes to disk ..."
            reconstructed.write(np_arr_slice.tobytes('F'))
            print "{0} level slice is done writing, {1}s is used".format(l, time.time() - one_slice_time)



def main():
    start_time = time.time()
    reconstruct_multi_reads("/data/bigbrain_blocks2/legend.txt", [3850, 3025, 3500], "/data/bigbrain_blocks2", "./recon_mreads.nii")
    print "time is {}".format(start_time - time.time())


if __name__ == '__main__':
    main()
import nibabel as nib
import numpy as np
import argparse
import os
import time


def create_histo(folder_path, min_val, max_val, num_bins=None):

    if num_bins is None:
        num_bins = int(max_val - min_val)    
        print num_bins

    histogram = {}

    for f in os.listdir(folder_path):
        
        im = nib.load(os.path.join(folder_path, f))
        data = im.get_data()

        hist, bins = np.histogram(data, num_bins, range=(min_val, max_val))

        count = 0

        for el in bins[0:-1]:
            try:
                histogram[el] += hist[count]
            except:
                histogram[el] = hist[count]
            count += 1

    return histogram.items()
        


def main():

    parser = argparse.ArgumentParser(description='Compute histogram')
    parser.add_argument('folder_path', type=str, help='folder path containing all of the splits')
    parser.add_argument('-b', '--bins', type=int, default=None, help="number of bins")

    args = parser.parse_args()

    min_time, max_time = 0

    
    min_v = None
    max_v = None

    for f in os.listdir(args.folder_path):
    
        im = nib.load(os.path.join(args.folder_path, f))
        data = im.get_data()

        t = time()
        tmp_min = np.min(data)
        min_time += time() - t

        t = time()
        tmp_max = np.max(data)
        max_time += time() - 1
    

        t = time()
        if min_v is None or tmp_min < min_v:
            min_v = tmp_min
        if max_v is None or tmp_max > max_v:
            max_v = tmp_max


    print create_histo(args.folder_path, min_v, max_v, args.bins)

if __name__ == "__main__":
    main()





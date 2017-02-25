import magic
from hdfs import Config
import nibabel as nib
from gzip import GzipFile
from io import BytesIO
import sys
import posixpath

class HDFSUtils:
    """Helper class for hdfs-related operations"""
    
    def __init__(self):
        self.client = Config().get_client()    


    #checks if filepath begins with hdfs://
    def is_hdfs_uri(self, uri):
        if 'hdfs://' in uri[:8]:
            return True
        return False

    #return the hdfs path. Temp solution
    def hdfs_path(self, uri):
        #if uri is not an hdfs uri, return the uri provided
        if not self.is_hdfs_uri(uri): return uri

        if self.client.root is None:
            #absolute path should begin at third slash since uris should be of format hdfs://host:port/path/to/file/or/folder
            n = 3
            path_split = uri.split('/')            
            return '/'.join(path_split[n:])

        return uri.split(self.client.root)[1]       

    #returns the image loaded into nibabel
    #function will also accept hdfs uris as filepath inputs
    def load_nifti(self, filepath, in_hdfs = None):
        if in_hdfs is None:
            in_hdfs = self.is_hdfs_uri(filepath)
            
        if in_hdfs:
            fh = None
            #gets hdfs path in the case an hdfs uri was provided
            filepath = self.hdfs_path(filepath)
            with self.client.read(filepath) as reader:
                stream = reader.read()
                if self.is_gzipped(filepath, stream[:2]):
                    fh = nib.FileHolder(fileobj=GzipFile(fileobj=BytesIO(stream)))
                else:
                    fh = nib.FileHolder(fileobj=BytesIO(stream))
            #TODO: handle minc files as well 
            return nib.Nifti1Image.from_file_map({'header':fh, 'image':fh})

        return nib.load(filepath)
            
                        
    #checks is file is gzipped
    #by default a filepath is required, however this function will not work
    #with a filepath if file is located on HDFS. Therefore, if file is located
    #on HDFS, the byte stream (must contain at least the first two bytes) will also need to be provided   
    def is_gzipped(self, filepath, buff=None):
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
            print('ERROR occured while attempting to determine if file is gzipped')
            sys.exit(1)

    #checks if filepath begins with file://
    def is_local(self, filepath):
        if 'file://' in filepath[0:8]:
            return True
        return False


    #copy file to hdfs
    #set ovrwrt to True if directory already exists and you wish all files to be replaced
    #a custom hdfs path or url for where to save the splits can be provided. If not provided, the files will be saved in a folder called 'hdfsutils_copied_files' located in hdfs root.
    #will save hdfs paths to file called filepaths.txt in hdfs_dir if save_path_to_file is set to True
    def copy_to_hdfs(self, local_filepath, hdfs_dir=None, ovrwrt=False, save_path_to_file=False):
        try:
            if hdfs_dir is None:
                hdfs_dir = 'hdfsutils_copied_files/'
                self.client.makedirs(hdfs_dir)
            
            #in case of hdfs uri    
            hdfs_dir = self.hdfs_path(hdfs_dir) 
            self.client.upload(hdfs_dir, local_filepath, overwrite=ovrwrt)
            
            if save_path_to_file:
                file_path = hdfs_dir + 'filepaths.txt'
                data = hdfs_dir + posixpath.basename(local_filepath) + '\n'
                
                #if file does not already exist, it will throw an error when trying to append
                try:
                    self.client.write(file_path, data, append=True)
                except:
                    #create the file as it does not already exist
                    self.client.write(file_path, data)
                
        except Exception as e:
            print('Error: Unable to copy files to HDFS\n', e)
            sys.exit(1)



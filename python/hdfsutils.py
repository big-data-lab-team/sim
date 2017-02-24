import magic
from hdfs import Config
import nibabel as nib
from gzip import GzipFile
from io import BytesIO

class HDFSUtils:
    """Helper class for hdfs-related operations"""
    
    def __init__(self):
        self.client = Config().get_client()    


    #checks if filepath begins with hdfs://
    def is_hdfs_uri(self, uri):
        if 'hdfs://' in uri[:8]:
            return True
        return False

    #return the hdfs path
    def hdfs_path(self, uri):
        if self.client.root is None:
            #absolute path should begin at third slash since uris should be of format hdfs://host:port/abspath
            n = 3
            path_split = uri.split('/')            
            return '/'.join(path_split[n:])

        return uri.split(self.client.root)[1]       

    #returns the image loaded into nibabel
    def get_slice(self, filepath, in_hdfs):
        if in_hdfs:
            fh = None
            with self.client.read(filepath) as reader:
                stream = reader.read()
                if self.is_gzipped(filepath, stream):
                    fh = nib.FileHolder(fileobj=GzipFile(fileobj=BytesIO(stream)))
                else:
                    fh = nib.FileHolder(fileobj=BytesIO(stream))
            
            return nib.Nifti1Image.from_file_map({'header':fh, 'image':fh})

        return nib.load(filepath)
            
                        
    #checks is file is gzipped
    #by default a filepath is required, however this function will not work
    #with a filepath if file is located on HDFS. Therefore, if file is located
    #on HDFS, the byte stream will also need to be provided   
    def is_gzipped(self, filepath, buff=None):
        mime = magic.Magic(mime=True)
        try:
            if buff is None:
                if 'gzip' in mime.from_file(filepath):
                    return True
                else:
                    return False
            else:
                if 'gzip' in mime.from_buffer(buff):
                    return True
                else:
                    return False
        except:
            print('ERROR occured while attempting to determine if file is gzipped')
            sys.exit(1)

    #checks if filepath begins with file://
    def is_local(self, filepath):
        if 'file://' in filepath[0:8]:
            return True
        return False

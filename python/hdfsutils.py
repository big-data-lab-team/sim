from hdfs import Config
import sys
import posixpath

class HDFSUtils:
    """Helper class for hdfs-related operations"""
   
    hdfs_flag = '_HDFSUTILS_FLAG_ '
    default_txt_file = 'filepaths.txt'
 
    #hdfs dir is accepted should you want another default dir to be use
    #accepting host and port as a current workaround to hdfscli not storing that information
    def __init__(self, hdfs_dir=None, host=None, port=None):
        self.client = Config().get_client()

        if hdfs_dir is None:
            self.hdfs_dir = 'hdfsutils_copied_files/'
        else:
            self.hdfs_dir = hdfs_dir    


    #checks if filepath begins with hdfs://
    def is_hdfs_uri(self, uri):
        if 'hdfs://' in uri[:8]:
            return True
        return False

    #checks if path is either an hdfs uri or contains the hdfs_flag
    def in_hdfs(self, path):
        if self.is_hdfs_uri(path):
            return True
        return self.hdfs_flag in path

    #return the hdfs path. Temp solution
    def hdfs_path(self, uri):
        #if uri is not an hdfs uri or doesn't contain the hdfs_flag, then it is either not in hdfs, or is already an hdfs_path
        #and does not need to be transformed 
        if not self.in_hdfs(uri): 
            return uri

        if self.hdfs_flag in uri: 
            return uri.split(self.hdfs_flag)[1]

        if self.client.root is None:
            #absolute path should begin at third slash since uris should be of format hdfs://host:port/path/to/file/or/folder
            n = 3
            path_split = uri.split('/')            
            return '/'.join(path_split[n:])

        return uri.split(self.client.root)[1]       
    
    
    #checks if filepath begins with file://
    def is_local(self, filepath):
        if 'file://' in filepath[0:8]:
            return True
        return False


    #copy file to hdfs
    #set ovrwrt to True if directory already exists and you wish all files to be replaced
    #a custom hdfs path or url for where to save the splits can be provided. If not provided, the files will be saved in the folder self.default_dir.
    #will save hdfs paths to file called filepaths.txt in hdfs_dir if save_path_to_file is set to True
    def copy_to_hdfs(self, local_filepath, hdfs_dir=None, ovrwrt=False, save_path_to_file=False):
        try:
            if hdfs_dir is None:
                hdfs_dir = self.default_dir
                self.client.makedirs(hdfs_dir)
            
            #in case of hdfs uri    
            hdfs_dir = self.hdfs_path(hdfs_dir) 
            self.client.upload(hdfs_dir, local_filepath, overwrite=ovrwrt)
            
            if save_path_to_file:
                file_path = hdfs_dir + default_txt_file
                
                #appending flag to hdfs_path as it would otherwise be difficult to determine which fs it's on with just a relative path
                data = self.hdfs_flag + hdfs_dir + posixpath.basename(local_filepath) + '\n'
                
                #if file does not already exist, it will throw an error when trying to append
                try:
                    self.client.write(file_path, data, append=True)
                except:
                    #create the file as it does not already exist
                    self.client.write(file_path, data)
                
        except Exception as e:
            print('Error: Unable to copy files to HDFS\n', e)
            sys.exit(1)

    #delete directory in hdfs dir given an hdfs url or path. If no url/path is provided self.default_dir will be deleted
    def delete_dir_hdfs(self, hdfs_dir=None, recursive=False):
        if hdfs_dir is None:
            hdfs_dir = self.default_dir
        else:
            hdfs_dir = self.hdfs_path(hdfs_dir)

        return self.client.delete(hdfs_dir, recursive=recursive)

    #will delete file at specified location. If no filepath is provided, it will attempt to deleted the default_txt_file located inside self.default_dir
    def delete_file_hdfs(self, filepath=None):
        if filepath is None:
            filepath = self.default_dir + default_text_file

        return self.client.delete(filepath)
        



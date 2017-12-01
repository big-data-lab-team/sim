import subprocess,os


def run_cmd(args_list):
        """
        run linux commands
        """
        # import subprocess
        print('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err


def hdfs_status():
    (ret, out, err) = run_cmd(['jps'])
    if ('SecondaryNameNode' not in out) & ('DataNode' not in out) & ('NameNode' not in out):
        return False
    else:
        return True
# list in hdfs to test if it is working or not
def hdfs_start():
    run_cmd(['start-dfs.sh'])
    if hdfs_status():
        print "HDFS has been started"
    else:
        print "HDFS could not be started. Please start it manually."

def directory_exists(dir_path):
    if os.path.isdir(dir_path) & (os.path.exists(dir_path)):
        return True
    else:
        return False

def tmpfs_directories():
    tmpfs_folder=""
    (ret, out, err) = run_cmd(['df','-aTh'])
    for item in out.split("\n"):
        if ("tmpfs" in item) & ("run/user" in item):
            for folder in item.split():
                if "run/user" in folder:
                    tmpfs_folder = folder
                    # returning first tmpfs folder found
                    break
    return tmpfs_folder
import boutiques, os, time, errno, tarfile, json

class Sim(object):
    
    def __init__(self, boutiques_descriptor, input_path, output_dir):

        self.boutiques_descriptor = boutiques_descriptor
        self.input_path = input_path
        self.output_dir = output_dir

    def create_tar_file(self, out_dir, tar_name, files):
        try:
            os.makedirs(out_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        with tarfile.open(os.path.join(out_dir, tar_name), "w") as tar:
            for f in files:
                tar.add(f)

    def write_invocation_file(self, invocation, invocation_file):

        # Note: the invocation file format will change soon
        json_invocation = json.dumps(invocation)

        # Writes invocation
        with open(invocation_file,"w") as f:
            f.write(json_invocation)

    def bosh_exec(self, invocation_file, mount=None):
        try:

            if mount is None:
                boutiques.execute("launch",self.boutiques_descriptor,invocation_file, "-x")
            else:
                boutiques.execute("launch", self.boutiques_descriptor, invocation_file, "-v",
                         "{0}:{0}".format(mount), "-x")
            result = 0
        except SystemExit as e:
            result = e.code
        return (result, "Empty log, Boutiques API doesn't return it yet.\n")

    def pretty_print(self, result):
        (label, (returncode, log)) = result
        status = "SUCCESS" if returncode == 0 else "ERROR"
        timestamp = str(int(time.time() * 1000))
        filename = "{0}.{1}.log".format(timestamp, label)
        with open(filename,"w") as f:
            f.write(log)
        print(" [ {0} ({1}) ] {2} - {3}".format(status, returncode, label, filename))

    def check_failure(self, result):
        (label, (returncode, log)) = result
        return True if returncode !=0 else False
    
    
    def write_BIDS_invocation(self, analysis_level, participant_label, invocation_file):
        
        invocation = {}
        invocation["bids_dir"] = self.input_path
        invocation["output_dir_name"] = self.output_dir
        if analysis_level == "participant":
            invocation["analysis_level"] = "participant"
            invocation["participant_label"] = participant_label
        elif analysis_level == "group":
            invocation["analysis_level"] = "group"

        self.write_invocation_file(invocation, invocation_file)

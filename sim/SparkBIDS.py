from bids.grabbids import BIDSLayout
import json, os, errno, subprocess, time, tarfile, shutil

class SparkBIDS(object):

    def __init__(self, boutiques_descriptor, bids_dataset, output_dir, options={}):
     
        self.boutiques_descriptor = os.path.join(os.path.abspath(boutiques_descriptor))
        self.bids_dataset = bids_dataset
        self.output_dir = output_dir
        
        # Includes: use_hdfs, skip_participant_analysis,
        # skip_group_analysis, skip_participants_file
        for option in list(options.keys()): setattr(self, option, options.get(option))

        # Check what will have to be done
        self.do_participant_analysis = self.supports_analysis_level("participant") \
                                                            and not self.skip_participant_analysis
        self.do_group_analysis = self.supports_analysis_level("group") \
                                                 and not self.skip_group_analysis
        self.skipped_participants = self.skip_participants_file.read().split() if self.skip_participants_file else []

        # Print analysis summary
        print("Computed Analyses: Participant [ {0} ] - Group [ {1} ]".format(str(self.do_participant_analysis).upper(),
                                                                              str(self.do_group_analysis).upper()))
     
        if len(self.skipped_participants):
            print("Skipped participants: {0}".format(self.skipped_participants)) 

    def run(self, sc):

        # Participant analysis
        if self.do_participant_analysis:
            # RDD creation from BIDS dataset
            rdd = self.create_RDD(sc)
            # rdd[0] is the participant label, rdd[1] is the data (if HDFS) or None

            # Participant analysis (done for all apps)
            mapped = rdd.filter(lambda x: self.get_participant_from_fn(x[0]) not in self.skipped_participants)\
                        .map(lambda x: self.run_participant_analysis(self.get_participant_from_fn(x[0]),
                                                                     x[1]))

            for result in mapped.collect():
                self.pretty_print(result)

        # Group analysis
        if self.do_group_analysis:
            group_result = self.run_group_analysis()
            self.pretty_print(group_result)

    def spark_required(self):
        return self.do_participant_analysis
            
    def supports_analysis_level(self,level):
        desc = json.load(open(self.boutiques_descriptor))
        analysis_level_input = None
        for input in desc["inputs"]:
            if input["id"] == "analysis_level":
                analysis_level_input = input
                break
        assert(analysis_level_input),"BIDS app descriptor has no input with id 'analysis_level'"
        assert(analysis_level_input.get("value-choices")),"Input 'analysis_level' of BIDS app descriptor has no 'value-choices' property"   
        return level in analysis_level_input["value-choices"]

    def create_RDD(self, sc):

        sub_dir="tar_files"

        layout = BIDSLayout(self.bids_dataset)
        participants = layout.get_subjects()    

        # Create RDD of file paths as key and tarred subject data as value
        if self.use_hdfs:
            for sub in participants:
                layout.get(subject=sub)
                self.create_tar_file(sub_dir, "sub-{0}.tar".format(sub), layout.files)

            return sc.binaryFiles("file://"+os.path.abspath(sub_dir))

        # Create RDD of tuples containing tuples of subject names and no data    
        it = iter(participants)
        empty_list = [None] * len(participants)
        list_participants = zip(it, empty_list)

        return sc.parallelize(list_participants)

    def create_tar_file(self, out_dir, tar_name, files):
        try:
            os.makedirs(out_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        with tarfile.open(os.path.join(out_dir, tar_name), "w") as tar:
            for f in files:
                tar.add(f)

    def pretty_print(self, result):
        (label, (log, returncode)) = result
        status = "SUCCESS" if returncode == 0 else "ERROR"
        timestamp = str(int(time.time() * 1000))
        filename = "{0}.{1}.log".format(timestamp, label)
        with open(filename,"w") as f:
            f.write(log)
        print(" [ {3} ({0}) ] {1} - {2}".format(returncode, label, filename, status))

    def write_invocation_file(self, analysis_level, participant_label, invocation_file):

        # Note: the invocation file format will change soon

        # Creates invocation object
        invocation = {}
        invocation["inputs"] = [ ]
        invocation["inputs"].append({"bids_dir": self.bids_dataset})
        invocation["inputs"].append({"output_dir_name": self.output_dir})
        if analysis_level == "participant":
            invocation["inputs"].append({"analysis_level": "participant"}) 
            invocation["inputs"].append({"participant_label": participant_label})
        elif analysis_level == "group":
            invocation["inputs"].append({"analysis_level": "group"})

        json_invocation = json.dumps(invocation)

        # Writes invocation
        with open(invocation_file,"w") as f:
            f.write(json_invocation)

    def get_bids_dataset(self, data, participant_label):

        filename = 'sub-{0}.tar'.format(participant_label)
        tmp_dataset = 'temp_dataset'    
        foldername = os.path.join(tmp_dataset, 'sub-{0}'.format(participant_label))

        # Save participant byte stream to disk
        with open(filename, 'w') as f:
            f.write(data)

        # Now extract data from tar
        tar = tarfile.open(filename)
        tar.extractall(path=foldername)
        tar.close()

        os.remove(filename)

        return os.path.join(tmp_dataset, os.path.abspath(self.bids_dataset))

    def run_participant_analysis(self, participant_label, data):

        if data is not None: # HDFS
            bids_dataset = self.get_bids_dataset(data,
                                                 participant_label)

        try:
            os.mkdir(self.output_dir)
        except OSError as exc: 
            if exc.errno == errno.EEXIST and os.path.isdir(self.output_dir):
                pass
            else:
                raise

        invocation_file = "./invocation-{0}.json".format(participant_label)
        self.write_invocation_file("participant",
                                   participant_label,
                                   invocation_file)

        exec_result = self.bosh_exec(invocation_file)
        os.remove(invocation_file)
        return (participant_label, exec_result)

    def run_group_analysis(self):
        invocation_file = "./invocation-group.json"
        self.write_invocation_file("group",
                                   None,
                                   invocation_file)
        exec_result = self.bosh_exec(invocation_file)
        os.remove(invocation_file)
        return ("group", exec_result)

    def bosh_exec(self, invocation_file):
        run_command = "bosh {0} -i {1} -e -d".format(self.boutiques_descriptor, invocation_file)
        result = None
        try:
            log = subprocess.check_output(run_command, shell=True, stderr=subprocess.STDOUT)
            result = (log, 0)
        except subprocess.CalledProcessError as e:
            result = (e.output, e.returncode)
        try:
            shutil.rmtree(label)
        except:
            pass
        return result

    def is_valid_file(parser, arg):
        if not os.path.exists(arg):
            parser.error("The file %s does not exist!" % arg)
        else:
            return open(arg, 'r')

    def get_participant_from_fn(self,filename):
        if filename.endswith(".tar"): return filename.split('-')[-1][:-4]
        return filename

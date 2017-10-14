from bids.grabbids import BIDSLayout
import boutiques, errno, json, os, shutil, subprocess, tarfile, time
from Sim import Sim

class SparkBIDS(Sim):

    def __init__(self, boutiques_descriptor, bids_dataset, output_dir, options={}):
     
        super(SparkBIDS, self).__init__(os.path.abspath(boutiques_descriptor), bids_dataset, output_dir)

        
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
                if self.check_failure(result):
                    # Disable Group Analysis if Participant Analysis Fails
                    self.do_group_analysis = False
                    print("ERROR# Participant analysis failed. Group analysis will be aborted.")


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

        layout = BIDSLayout(self.input_path)
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

        return os.path.join(tmp_dataset, os.path.abspath(self.input_path))

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
        self.write_BIDS_invocation("participant",
                                   participant_label,
                                   invocation_file)

        exec_result = self.bosh_exec(invocation_file, os.path.dirname(os.path.abspath(self.input_path)))
        os.remove(invocation_file)
        return (participant_label, exec_result)

    def run_group_analysis(self):
        invocation_file = "./invocation-group.json"
        self.write_BIDS_invocation("group",
                                   None,
                                   invocation_file)
        exec_result = self.bosh_exec(invocation_file, os.path.dirname(os.path.abspath(self.input_path)))
        os.remove(invocation_file)
        return ("group", exec_result)


    def get_participant_from_fn(self,filename):
        if filename.endswith(".tar"): return filename.split('-')[-1][:-4]
        return filename

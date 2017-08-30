#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from bids.grabbids import BIDSLayout
import argparse, json, os, errno, subprocess, time, tarfile, shutil

def supports_analysis_level(boutiques_descriptor, level):
    desc = json.load(open(boutiques_descriptor))
    analysis_level_input = None
    for input in desc["inputs"]:
        if input["id"] == "analysis_level":
            analysis_level_input = input
            break
    assert(analysis_level_input),"BIDS app descriptor has no input with id 'analysis_level'"
    assert(analysis_level_input.get("value-choices")),"Input 'analysis_level' of BIDS app descriptor has no 'value-choices' property"   
    return level in analysis_level_input["value-choices"]

def create_RDD(bids_dataset_root,sc, is_tar, sub_dir='tar_participants'):
    layout = BIDSLayout(bids_dataset_root)
    participants = layout.get_subjects()    

    # Create RDD of file paths as key and tarred subject data as value
    if is_tar:
        try:
            os.makedirs(sub_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        
        for sub in participants:
            files = layout.get(subject=sub)
            
            with tarfile.open(os.path.join(sub_dir, "sub-{0}.tar".format(sub)), "w") as tar:
                for f in files:
                    tar.add(f.filename)

        return sc.binaryFiles(sub_dir)

    # Create RDD of containing keys of subject names and no data    
    it = iter(participants)
    empty_list = [None] * len(participants)
    list_participants = zip(it, empty_list)

    return sc.parallelize(list_participants)


def pretty_print(result):
    label, log, returncode = result
    if returncode == 0:
        print(" [ SUCCESS ] {0}".format(label))
    else:
        timestamp = str(int(time.time() * 1000))
        filename = "{0}.{1}.err".format(timestamp, label)
        with open(filename,"w") as f:
            f.write(log)
        f.close()
        print(" [ ERROR ({0}) ] {1} - {2}".format(returncode, label, filename))

def write_invocation_file(bids_dataset, output_dir, analysis_level, participant_label, invocation_file):

    # Note: the invocation file format will change soon
    
    # Creates invocation object
    invocation = {}
    invocation["inputs"] = [ ]
    invocation["inputs"].append({"bids_dir": bids_dataset})
    invocation["inputs"].append({"output_dir_name": output_dir})
    if analysis_level == "participant":
        invocation["inputs"].append({"analysis_level": "participant"}) 
        invocation["inputs"].append({"participant_label": participant_label})
    elif analysis_level == "group":
        invocation["inputs"].append({"analysis_level": "group"})
        
    json_invocation = json.dumps(invocation)

    # Writes invocation
    with open(invocation_file,"w") as f:
        f.write(json_invocation)

def get_bids_dataset(bids_dataset, data, participant_label):

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

    return os.path.join(tmp_dataset, os.path.abspath(bids_dataset))
    

def run_participant_analysis(boutiques_descriptor, bids_dataset, participant_label, output_dir, data):

    if data is not None:
        bids_dataset = get_bids_dataset(bids_dataset, data, participant_label)

    invocation_file = "./invocation-{0}.json".format(participant_label)
    write_invocation_file(bids_dataset, output_dir, "participant", participant_label, invocation_file)
    return bosh_exec(boutiques_descriptor, invocation_file, "sub-{0}".format(participant_label))

def run_group_analysis(boutiques_descriptor, bids_dataset, output_dir):
    invocation_file = "./invocation-group.json"
    write_invocation_file(bids_dataset, output_dir, "group", None, invocation_file)
    return bosh_exec(boutiques_descriptor, invocation_file, "group")

def bosh_exec(boutiques_descriptor, invocation_file, label):
    run_command = "localExec.py {0} -i {1} -e -d".format(boutiques_descriptor, invocation_file)
    result = None
    try:
        log = subprocess.check_output(run_command, shell=True, stderr=subprocess.STDOUT)
        result = (label, log, 0)
    except subprocess.CalledProcessError as e:
        result = (label, e.output, e.returncode)
    #os.remove(invocation_file)

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

def get_participant_from_fn(filename):
    if filename.endswith(".tar"): return filename.split('-')[-1][:-4]
    return filename

def main():

    # Arguments parsing
    parser=argparse.ArgumentParser()
    # Required inputs
    parser.add_argument("bids_app_boutiques_descriptor", help="Boutiques descriptor of the BIDS App that will process the dataset.")
    parser.add_argument("bids_dataset", help="BIDS dataset to be processed.")
    parser.add_argument("output_dir", help="Output directory.")
    # Optional inputs
    # Analysis options
    parser.add_argument("--skip-session-analysis", action = 'store_true', help="Skips session analysis.")
    parser.add_argument("--skip-participant-analysis", action = 'store_true', help="Skips participant analysis.")
    parser.add_argument("--skip-group-analysis", action = 'store_true', help="Skips groups analysis.")
    parser.add_argument("--skip-participants", metavar="FILE", type=lambda x: is_valid_file(parser, x), help="Skips participant labels in the text file.")
    parser.add_argument("--skip-sessions", metavar="FILE", type=lambda x: is_valid_file(parser, x), help="Skips session labels in the text file, for all participants.")
    # Performance options
    parser.add_argument("--tar", action = 'store_true', help="Creates an RDD of tarred participants.")
    args=parser.parse_args()

    # Required inputs
    boutiques_descriptor = os.path.join(os.path.abspath(args.bids_app_boutiques_descriptor))
    bids_dataset = args.bids_dataset
    output_dir = args.output_dir
    is_tar = args.tar

    # Optional inputs
    do_subject_analysis = supports_analysis_level(boutiques_descriptor, "session") and not args.skip_session_analysis
    do_participant_analysis = supports_analysis_level(boutiques_descriptor, "participant") and not args.skip_participant_analysis
    do_group_analysis = supports_analysis_level(boutiques_descriptor,"group") and not args.skip_group_analysis
    
    skipped_participants = args.skip_participants.read().split() if args.skip_participants else []
    skipped_sessions = args.skip_sessions.read().split() if args.skip_sessions else []

    # Print analysis summary
    print("Computed Analyses: Session [ {0} ] - Participant [ {1} ] - Group [ {2} ]".format(str(do_subject_analysis).upper(),
                                                                                            str(do_participant_analysis).upper(),
                                                                                            str(do_group_analysis).upper()))
    if len(skipped_sessions):
        print("Skipped sessions: {0}".format(skipped_sessions)) 
    if len(skipped_participants):
        print("Skipped participants: {0}".format(skipped_participants)) 

    # Return if there is nothing to do
    if not ( do_subject_analysis or do_participant_analysis or do_group_analysis ):
        sys.exit(0)
        
    # Spark initialization
    conf = SparkConf().setAppName("BIDS pipeline")
    sc = SparkContext(conf=conf)


    # RDD creation from BIDS dataset
    rdd = create_RDD(bids_dataset, sc, is_tar)
    # Participant analysis (done for all apps)
    mapped = rdd.filter(lambda x: get_participant_from_fn(x[0]) not in skipped_participants)\
                .map(lambda x: run_participant_analysis(boutiques_descriptor, bids_dataset, get_participant_from_fn(x[0]), output_dir, x[1]))


    for result in mapped.collect():
        pretty_print(result)

    # Group analysis
    if do_group_analysis:
        group_result = run_group_analysis(boutiques_descriptor, bids_dataset, output_dir)
        pretty_print(group_result)
        
# Execute program
if  __name__ == "__main__":
    main()

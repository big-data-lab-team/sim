import errno, json, os, sys
from Sim import Sim

class SparkNeuroImage(Sim):

    def __init__(self, boutiques_descriptor, input_dir, output_dir, index, use_hdfs, options={}):
     
        super(SparkNeuroImage, self).__init__(os.path.abspath(boutiques_descriptor), input_dir, output_dir)
        
        self.index = index
        
        self.use_hdfs = use_hdfs

        self.input_id = self.output_id = None    
        self.option_ids = list(options.keys())

        # Contains any arguments required to execute the pipeline. Option names 
        # must match that of its respective id in the boutiques descriptor.
        # If the input id or output id is not called "input_file" or "output_file", respectively,
        # in the boutiques descriptor, please include input_id and output_id with its 
        # id name as option argument   
        for option in self.option_ids : setattr(self, option, options.get(option))

        self.chunk_fns = self.index.read().split() if self.index else []
        
        # Print analysis summary
        print("*****Computed Analyses*****")

        for arg_id, val in options.items():
            if arg_id != "input_id" and arg_id != "output_id":
                print("{0} [ {1} ]".format(str(arg_id).upper(),str(val).upper()))

        print("***************************")

    def run(self, sc):

        # RDD creation from image chunk directory
        rdd = self.create_RDD(sc)
        # rdd[0] is the filename, rdd[1] is the data

        # Filter out unused data and execute command on each chunk
        mapped = rdd.filter(lambda x: os.path.basename(x[0]) in self.chunk_fns)\
                    .map(lambda x: self.save_data_locally(os.path.basename(x[0]),x[1]))\
                    .map(lambda x: self.run_analysis(x[0]))

        for result in mapped.collect():
            self.pretty_print(result)

    def save_data_locally(self, filename, data):
        with open(filename, 'w') as f:
            f.write(data)
        return (filename, "")

    def run_analysis(self, chunk_filename):

        try:
            os.mkdir(self.output_dir)
        except OSError as exc: 
            if exc.errno == errno.EEXIST and os.path.isdir(self.output_dir):
                pass
            else:
                raise
    
        desc = json.load(open(self.boutiques_descriptor))
        
        pip_name = "-".join(str(desc["name"]).split())
        invocation_file = "./invocation-{0}-{1}.json".format(pip_name, os.getpid())

        self.write_invocation(chunk_filename, invocation_file)

        exec_result = self.bosh_exec(invocation_file, os.path.dirname(os.path.abspath(self.input_path)))
        os.remove(invocation_file)
        os.remove(os.path.basename(chunk_filename))
        return ("result", exec_result)
            
    def create_RDD(self, sc):
        if self.use_hdfs:
            return sc.binaryFiles(self.input_path)
        else:
            return sc.binaryFiles("file://" + os.path.abspath(self.input_path))


    def write_invocation(self, chunk_filename, invocation_file):
        
        invocation = {}

        if self.input_id is not None:
            invocation[self.input_id] = chunk_filename
        else:
            invocation["input_file"] = chunk_filename
        
        output_fn = os.path.join(self.output_dir, chunk_filename)
    
        if self.output_id is not None:
            invocation[self.output_id] = output_fn
        else:
            invocation["output_file"] = output_fn

        desc_ids = self.get_descriptor_ids()

        for opt in self.option_ids:
            if opt != "input_id" and opt != "output_id":
                if opt in desc_ids:
                    invocation[opt] = getattr(self, opt)
                else:
                    print("ERROR: Descriptor input id {0} does not exist in descriptor".format(opt))
                    sys.exit(1)

        self.write_invocation_file(invocation, invocation_file)

    def get_descriptor_ids(self):
        desc = json.load(open(self.boutiques_descriptor))

        desc_ids = []

        for inpt in desc["inputs"]:
            desc_ids.append(str(inpt["id"]))
            
        return desc_ids

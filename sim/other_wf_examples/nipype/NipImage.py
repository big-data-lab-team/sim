from nipype import Workflow, MapNode, Node, Function
from nipype.interfaces.utility import IdentityInterface, Function
import os, json, time
from sim import Sim

class NipImage(Sim.Sim):

    def __init__(self, boutiques_descriptor, input_dir, output_dir, index, options={}):
     
        super(NipImage, self).__init__(os.path.abspath(boutiques_descriptor), os.path.abspath(input_dir), os.path.abspath(output_dir))
        
        self.index = os.path.abspath(index)

        self.input_id = self.output_id = None
        self.option_ids = list(options.keys())

        # Contains any arguments required to execute the pipeline. Option names 
        # must match that of its respective id in the boutiques descriptor.
        # If the input id or output id is not called "input_file" or "output_file", respectively,
        # in the boutiques descriptor, please include input_id and output_id with its 
        # id name as option argument   
        for option in self.option_ids : setattr(self, option, options.get(option))

        self.chunk_fns = open(self.index, "r").read().split() if self.index else []

        # Print analysis summary
        print("*****Computed Analyses*****")

        for arg_id, val in options.items():
            if arg_id != "input_id" and arg_id != "output_id":
                print("{0} [ {1} ]".format(str(arg_id).upper(),str(val).upper()))

        print("***************************")

    def run(self):

        wf = Workflow('sim_nipype_app')
        wf.base_dir = os.getcwd()

        chunks = self.chunk_fns

        analysis = MapNode(Function(input_names=['nip', 
                                    'chunk_filename','working_dir'],
                                  output_names=['result'],
                                  function=run_analysis),
                                  iterfield=['chunk_filename'],
                                  name='run_analysis')

        wf.add_nodes([analysis])            
        
        analysis.inputs.chunk_filename = chunks
        analysis.inputs.nip = self
        analysis.inputs.working_dir = os.getcwd()

            
        eg = wf.run()

       
        # Convert to dictionary to more easily extract results 
        node_names = [i.name for i in eg.nodes()]
        result_dict = dict(zip(node_names, eg.nodes()))

        for res in result_dict['run_analysis'].result.outputs.get('result'):
            self.pretty_print(res)

        

    def write_invocation(self, chunk_filename, invocation_file):

        invocation = {}

        in_file = os.path.join(self.input_path, chunk_filename)    
    
        if self.input_id is not None:
            invocation[self.input_id] = in_file
        else:
            invocation["input_file"] = in_file

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

def run_analysis(nip, chunk_filename, working_dir):
    import os, json

    out_key = "result"

    desc = json.load(open(nip.boutiques_descriptor))

    pip_name = "-".join(str(desc["name"]).split())
    invocation_file = "./invocation-{0}-{1}.json".format(pip_name, chunk_filename.split('.')[0])

    nip.write_invocation(chunk_filename, invocation_file)

    exec_result = nip.bosh_exec(invocation_file, working_dir)
    os.remove(invocation_file)

    return (out_key, exec_result)



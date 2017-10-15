from nipype import Workflow, MapNode, Node, Function
from nipype.interfaces.utility import IdentityInterface, Function
import os, json, time
from sim import Sim

class NipBIDS(Sim.Sim):

    def __init__(self, boutiques_descriptor, bids_dataset, output_dir, options={}):
     
        super(NipBIDS, self).__init__(os.path.abspath(boutiques_descriptor),
                         os.path.abspath(bids_dataset),
                         os.path.abspath(output_dir))
        
        # Includes: skip_participant_analysis,
        # skip_group_analysis, skip_participants_file
        for option in list(options.keys()): setattr(self, option, options.get(option))

        # Check what will have to be done
        self.do_participant_analysis = self.supports_analysis_level("participant") \
                                                            and not self.skip_participant_analysis
        self.do_group_analysis = self.supports_analysis_level("group") \
                                                 and not self.skip_group_analysis
        self.skipped_participants = open(self.skip_participants_file, "r").read().split() if self.skip_participants_file else []

        
        # Print analysis summary
        print("Computed Analyses: Participant [ {0} ] - Group [ {1} ]".format(str(self.do_participant_analysis).upper(),
                                                                              str(self.do_group_analysis).upper()))
     
        if len(self.skipped_participants):
            print("Skipped participants: {0}".format(self.skipped_participants)) 


    def run(self):

        wf = Workflow('bapp')
        wf.base_dir = os.getcwd()

        # group analysis can be executed if participant analysis is skipped
        p_analysis = None

        # Participant analysis
        if self.do_participant_analysis:

            participants = Node(Function(input_names=['nip'],
                                            output_names=['out'],
                                            function=get_participants),
                                    name='get_participants')
            participants.inputs.nip = self



            p_analysis = MapNode(Function(input_names=['nip', 'analysis_level', 
                                      'participant_label','working_dir'],
                                      output_names=['result'],
                                      function=run_analysis),
                                      iterfield=['participant_label'],
                                      name='run_participant_analysis')

            wf.add_nodes([participants])            
            wf.connect(participants, 'out', p_analysis, 'participant_label')
            
            p_analysis.inputs.analysis_level = 'participant'
            p_analysis.inputs.nip = self
            p_analysis.inputs.working_dir = os.getcwd()


        # Group analysis
        if self.do_group_analysis:
            groups = Node(Function(input_names=['nip', 'analysis_level', 
                                'working_dir', 'dummy_token'],
                                output_names=['g_result'],
                                function=run_analysis),
                                name='run_group_analysis')

            groups.inputs.analysis_level = 'group'
            groups.inputs.nip = self
            groups.inputs.working_dir = os.getcwd()

        
            if p_analysis is not None:
                wf.connect(p_analysis, 'result', groups, 'dummy_token')
            else:
                wf.add_nodes([groups])
            
        eg = wf.run()

       
        # Convert to dictionary to more easily extract results 
        node_names = [i.name for i in eg.nodes()]
        result_dict = dict(zip(node_names, eg.nodes()))

        if self.do_participant_analysis:        
            for res in result_dict['run_participant_analysis'].result.outputs.get('result'):
                self.pretty_print(res)

        if self.do_group_analysis:
            self.pretty_print(result_dict['run_group_analysis'].result.outputs.g_result)
        
            
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

def run_analysis(nip, analysis_level, working_dir, participant_label=None, dummy_token=None):
    import os

    out_key = None

    if analysis_level == "group":
        invocation_file = "./invocation-group.json"
        out_key = "group"
    else:
        invocation_file = "./invocation-{0}.json".format(participant_label)
        out_key = "participant_label"

    nip.write_BIDS_invocation(analysis_level, participant_label, invocation_file)
    exec_result = nip.bosh_exec(invocation_file, working_dir)
    os.remove(invocation_file)

    return (out_key, exec_result)



def get_participants(nip):

    from bids.grabbids import BIDSLayout

    layout = BIDSLayout(nip.input_path)
    participants = layout.get_subjects()    
    
    return list(set(participants) - set(nip.skipped_participants))

#!/usr/bin/env python

import taxi.jobs
import taxi.local.local_taxi as local_taxi

import os, sys
import platform

## local_taxi should specify:
# - "flow_binary"

flow_arg_list = [
    'Ns',       ## Number of lattice points in spatial direction   
    'Nt',       ## Number of lattice points in temporal direction
    'epsilon',  ## Step size for numerical integration of flow
    'tmax',     ## Terminate flow at t=tmax
    'minE',     ## Terminate flow when E=minE
    'mindE',    ## Terminate flow when dE=mindE
    'loadg',    ## Path of gauge configuration to load
]

flow_input_template = """
prompt 0
    
nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}
    
epsilon {epsilon}
tmax {tmax}
minE {minE}
mindE {mindE}
    
reload_serial {loadg}
forget

EOF
"""

class FlowJob(taxi.jobs.Job):
    def __init__(self, req_time, tmax, minE=0, mindE=0.0, epsilon=0.1, **kwargs):
        super(FlowJob, self).__init__(req_time=req_time, **kwargs)

        # Physical parameters
        self.tmax = tmax
        self.minE = minE
        self.mindE = mindE
        self.epsilon = epsilon

        # Don't run trivial flows
        if tmax == 0:
            assert minE != 0 or mindE != 0

        # Filesystem
        self.generate_outfilename()        

    def generate_outfilename(self):
        self.fout = "flow_" + self.ensemble_name + "_{}".format(self.traj)
        
    def compile(self):
        super(FlowJob, self).compile()

        # Package in to JSON forest format
        self.compiled.update({
            'task_type' : None, ## abstract task
            'task_args': { k: getattr(self, k, default=None) for k in flow_arg_list },
        })

class FileFlowJob(FlowJob):
    def __init__(self, **kwargs):
        for arg in flow_arg_list:
            setattr(self, arg, kwargs.get(arg, None))

        parsed_params = self.parse_params_from_loadg()
        for param in ('Ns', 'Nt', 'traj', 'ensemble_name'):
            if getattr(self, param) is None:
                setattr(self, param, parsed_params[param])

        # Call superconstructor
        super(FileFlowJob, self).__init__(req_time=self.req_time, tmax=self.tmax, minE=self.minE,
                                          mindE=self.mindE, epsilon=self.epsilon, **kwargs)
    
    def parse_params_from_loadg(self):
        # e.g., GaugeSU4_12_6_7.75_0.128_0.128_1_0
        words = os.path.basename(self.loadg).split('_')
        
        return {'Ns' : int(words[1]),
                'Nt' : int(words[2]),
                'traj': int(words[-1]),
                'ensemble_name' : '_'.join(words[1:-1]),
        }

class HMCAuxFlowJob(FlowJob):
    def __init__(self, hmc_job, **kwargs):
        for arg in flow_arg_list:
            setattr(self, arg, kwargs.get(arg, None))

        self.depends_on = [hmc_job]

        self.Ns = hmc_job.Ns
        self.Nt = hmc_job.Nt
        self.traj = hmc_job.traj
        self.ensemble_name = hmc_job.ensemble_name

        self.loadg = hmc_job.saveg

        super(HMCAuxFlowJob, self).__init__(req_time=self.req_time, tmax=self.tmax, minE=self.minE,
            mindE=self.mindE, epsilon=self.epsilon, **kwargs)


    


class FlowRunner(taxi.jobs.TaskRunner):
    def __init__(self, **kwargs):
        self.binary = local_taxi.flow_binary

        for arg in flow_arg_list:
            setattr(self, arg, kwargs.get(arg, None))

        ## Sanitize paths
        self.loadg = taxi.jobs.sanitized_path(self.loadg)


    def to_dict(self):
        return { k: getattr(self, k, default=None) for k in flow_arg_list }

    def build_input_string(self):
        input_dict = self.to_dict()

        return flow_input_template.format(**input_dict)

    def verify_output(self):
        pass

    def execute(self):
        if self.use_mpi:
            exec_str = local_taxi.mpirun_str.format(self.cores)
        else:
            exec_str = "./"

        input_str = self.build_input_string()

        exec_str += self.binary + " << EOF >> {fout}\n".format(fout=self.fout)
        exec_str += input_str

        os.system(exec_str)

        self.verify_output()

## Runner stubs needed for the job-->runner factory to find them, even though functionality is identical
class FileFlowRunner(FlowRunner):
    pass

class HMCAuxFlowRunner(FlowRunner):
    pass

def flow_jobs_for_hmc_jobs(hmc_stream, req_time, tmax, minE=0, mindE=0, epsilon=.01, start_at_count=10):
    flow_jobs = []
    for hmc_job in hmc_stream:
        ## TODO: "HMCJob" superclass isn't used at the moment.
        # if not isinstance(hmc_job, HMCJob):
        #     continue
        if hmc_job.count >= start_at_count:
            flow_jobs.append(HMCAuxFlowJob(hmc_job, req_time=req_time,
                                           tmax=tmax, minE=minE, mindE=mindE, epsilon=epsilon))
    return flow_jobs

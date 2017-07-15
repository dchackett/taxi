# -*- coding: utf-8 -*-
"""
"""

import json
import sqlite3
import os

# numpy not available on Janus, use random package
from random import seed, randint

from tasks import CopyJob, SpawnJob, RespawnJob
from tasks import HMCJob, HMCAuxJob # , NstepAdjustor
from tasks import PureGaugeORAJob

from tasks import SpectroJob, FileSpectroJob, HMCAuxSpectroJob
from tasks import FlowJob, FileFlowJob, HMCAuxFlowJob
from tasks import HRPLJob, FileHRPLJob, HMCAuxHRPLJob



### Utility
def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)
    
        
### HMC stream-maker convenience functions
def make_hmc_job_stream(Ns, Nt, beta, k4, k6, N_configs, nsteps, starter, req_time,
                         start_count=0, N_traj=10, N_traj_safe=5,
                         gammarat=125., label='1',
                         nsteps2=None, shift=0.2, nsteps_gauge=6,
                         streamseed=None, seeds=None,
                         enable_metropolis=True, hmc_class=HMCJob):

    assert issubclass(hmc_class, HMCJob), "hmc_class must be an HMCJob or a subclass thereof, not {hmc}".format(hmc=str(hmc_class))
    
    # Randomly generate a different seed for each hmc run
    if streamseed is None:
        streamseed = hash((Ns, Nt, beta, gammarat, k4, k6, label))
    seed(streamseed%10000)
    
    hmc_stream = []
    for cc, count in enumerate(range(start_count, start_count+N_configs)):
        if seeds is None:        
            job_seed = randint(0, 9999)
        else:
            job_seed = seeds[cc]
            
        new_job = hmc_class(Ns=Ns, Nt=Nt, beta=beta, gammarat=gammarat, k4=k4, k6=k6,
                         label=label, count=count, req_time=req_time, N_traj=N_traj,
                         N_traj_safe=N_traj_safe, nsteps=nsteps, nsteps2=nsteps2,
                         shift=shift, nsteps_gauge=nsteps_gauge,
                         starter=starter, seed=job_seed, enable_metropolis=enable_metropolis)
        if isinstance(starter, HMCJob):
            new_job.depends_on.append(starter)
        starter = new_job
        hmc_stream.append(new_job)
        
    # Let the first job know it's the beginning of a new branch/sub-trunk
    hmc_stream[0].branch_root = True
        
    return hmc_stream

    
def spectro_jobs_for_hmc_jobs(hmc_stream, req_time,
                            r0, irrep, kappa=None, cgtol=None,
                            screening=False, p_plus_a=False,
                            save_prop=False, start_at_count=10):
    spectro_jobs = []
    for hmc_job in hmc_stream:
        if not isinstance(hmc_job, HMCJob):
            continue
        if hmc_job.count >= start_at_count:
            spectro_jobs.append(HMCAuxSpectroJob(hmc_job, req_time=req_time,
                                                irrep=irrep, r0=r0, kappa=kappa, cgtol=cgtol,
                                                screening=screening, p_plus_a=p_plus_a, save_prop=save_prop))
    return spectro_jobs
    
    
def flow_jobs_for_hmc_jobs(hmc_stream, req_time, tmax, minE=0, mindE=0, epsilon=.01, start_at_count=10):
    flow_jobs = []
    for hmc_job in hmc_stream:
        if not isinstance(hmc_job, HMCJob):
            continue
        if hmc_job.count >= start_at_count:
            flow_jobs.append(HMCAuxFlowJob(hmc_job, req_time=req_time,
                                           tmax=tmax, minE=minE, mindE=mindE, epsilon=epsilon))
    return flow_jobs
    
    
def hrpl_jobs_for_hmc_jobs(hmc_stream, start_at_count=0):
    hrpl_jobs = []
    for hmc_job in hmc_stream:
        if not isinstance(hmc_job, HMCJob):
            continue
        if hmc_job.count >= start_at_count:
            hrpl_jobs.append(HMCAuxHRPLJob(hmc_job))
    return hrpl_jobs


### Pure gauge stream-maker convenience functions
def make_ora_job_stream(Ns, Nt, beta,
                        N_configs,
                        starter, req_time,
                        nsteps=4, qhb_steps=1,
                        start_count=0, N_traj=100, warms=0,
                        label='1',
                        streamseed=None, seeds=None,
                        ora_class=PureGaugeORAJob):

    assert issubclass(ora_class, PureGaugeORAJob), "hmc_class must be an PureGaugeORAJob or a subclass thereof, not {ora}".format(ora=str(ora_class))
    
    # Randomly generate a different seed for each hmc run
    if streamseed is None:
        streamseed = hash((Ns, Nt, beta, label))
    seed(streamseed%10000)
    
    ora_stream = []
    for cc, count in enumerate(range(start_count, start_count+N_configs)):
        if seeds is None:        
            job_seed = randint(0, 9999)
        else:
            job_seed = seeds[cc]
            
        new_job = ora_class(Ns=Ns, Nt=Nt, beta=beta,
                         label=label, count=count, req_time=req_time, N_traj=N_traj,
                         nsteps=nsteps, qhb_steps=qhb_steps, warms=warms,
                         starter=starter, seed=job_seed)
        
        if isinstance(starter, PureGaugeORAJob):
            new_job.depends_on.append(starter)
        starter = new_job
        ora_stream.append(new_job)
        
        warms = 0 # Only do warmups at beginning of stream
        
    # Let the first job know it's the beginning of a new branch/sub-trunk
    ora_stream[0].branch_root = True
        
    return ora_stream        


def flow_jobs_for_ora_jobs(ora_stream, req_time, tmax, minE=0, mindE=0, epsilon=.01, start_at_count=10):
    flow_jobs = []
    for ora_job in ora_stream:
        if not isinstance(ora_job, PureGaugeORAJob):
            continue
        if ora_job.count >= start_at_count:
            new_job = FileFlowJob(ora_job.saveg, req_time=req_time,
                        tmax=tmax, minE=minE, mindE=mindE, epsilon=epsilon)
            new_job.depends_on = [ora_job]
            flow_jobs.append(new_job)
    return flow_jobs


### Tools for running measurements on pre-existing gauge files
def spectro_jobs_for_gaugefiles(gaugefiles, req_time, r0, irrep, cgtol=None,
                                screening=False, p_plus_a=False, do_baryons=False, save_prop=False):
    spectro_jobs = []
    for gfn in map(os.path.abspath, gaugefiles):
        assert os.path.exists(gfn), "Gaugefile {gfn} does not exist to run spectroscopy on".format(gfn=gfn)
        spectro_jobs.append(FileSpectroJob(loadg=gfn, req_time=req_time, irrep=irrep, r0=r0, cgtol=cgtol,
                                           screening=screening, p_plus_a=p_plus_a,
                                           do_baryons=do_baryons, save_prop=save_prop))
    return spectro_jobs


def flow_jobs_for_gaugefiles(gaugefiles, req_time, tmax, minE=0, mindE=0, epsilon=.01):
    flow_jobs = []
    for gfn in map(os.path.abspath, gaugefiles):
        assert os.path.exists(gfn), "Gaugefile {gfn} does not exist to run flow on".format(gfn=gfn)
        flow_jobs.append(FileFlowJob(gfn, req_time=req_time,
                                     tmax=tmax, minE=minE, mindE=mindE, epsilon=epsilon))
    return flow_jobs
    
    
def hrpl_jobs_for_gaugefiles(gaugefiles):
    hrpl_jobs = []
    for gfn in map(os.path.abspath, gaugefiles):
        assert os.path.exists(gfn), "Gaugefile {gfn} does not exist to measure HRPL for".format(gfn=gfn)
        hrpl_jobs.append(FileHRPLJob(gfn))
    return hrpl_jobs



### Tools for organizing output files
def parse_params_from_fn(fn):
    words = os.path.basename(fn).split('_')
    if words[0] in ['out', 'flow', 'hrpl', 'GaugeSU4']:
        # e.g., out_18_6_7.75_0.126_0.125_1_2
        return {'Ns' : int(words[1]),
                'Nt' : int(words[2]),
                'beta' : float(words[3]),
                'k4' : float(words[4]),
                'k6' : float(words[5])}
    elif 'spec' in words[0] or 'Prop' in words[0]:
        # e.g., xspecpa_r6_f_18_6_7.75_0.126_0.125_1_10
        return {'Ns' : int(words[3]),
                'Nt' : int(words[4]),
                'beta' : float(words[5]),
                'k4' : float(words[6]),
                'k6' : float(words[7])}
    else:
        raise Exception("Can't parse filename '{fn}' to make copy jobs".format(fn=fn))
                
def parse_params_from_HMCJob(hmc_job):
    return {'Ns' : hmc_job.Ns,
            'Nt' : hmc_job.Nt,
            'beta' : hmc_job.beta,
            'k4' : hmc_job.k4,
            'k6' : hmc_job.k6}
    
def parse_params_from_PGJob(pg_job):
    return {'Ns' : pg_job.Ns,
            'Nt' : pg_job.Nt,
            'beta' : pg_job.beta}
        
def copy_jobs_to_sort_output(job_pool, data_dir=None, gauge_dir=None, prop_dir=None):
    copy_jobs = []
    for job in job_pool:
        # Must find volume, couplings for multirep file structure
        if isinstance(job, HMCJob):
            parsed_params = parse_params_from_HMCJob(job)
        elif isinstance(job, HMCAuxJob):
            parsed_params = parse_params_from_HMCJob(job.hmc_job)
        elif isinstance(job, PureGaugeORAJob):
            parsed_params = parse_params_from_PGJob(job)
            
        elif hasattr(job, 'fout'):
            # Duck typing -- parse necessary parameters out of whatever file exists
            parsed_params = parse_params_from_fn(job.fout)
        elif hasattr(job, 'saveg'):
            # Duck typing -- rarely, something might save a gauge file but no output file
            parsed_params = parse_params_from_fn(job.saveg)
        else:            
            # No outputs to worry about
            continue
        
        # Build multirep path structure
        vol_subdir = '/{Ns}x{Nt}/'.format(**parsed_params)
        if parsed_params.has_key('k4') and parsed_params.has_key('k6'):
            coupling_subdir = '/{beta}/{k4}_{k6}/'.format(**parsed_params)
        else:
            coupling_subdir = '/{beta}/'.format(**parsed_params)
        
        ## Copy jobs for outputs from binaries
        if data_dir is not None:
            fout_path = data_dir + vol_subdir

            # Output-type-specific path structure
            if   isinstance(job, HMCJob):
                fout_path += 'hmc/'
            elif isinstance(job, PureGaugeORAJob):
                fout_path += 'ora/'
            elif isinstance(job, SpectroJob):
                if job.irrep == 'f':
                    fout_path += 'spec4_r{r0:g}/'.format(r0=job.r0)
                elif job.irrep == 'as2':
                    fout_path += 'spec6_r{r0:g}/'.format(r0=job.r0)
            elif isinstance(job, FlowJob):
                fout_path += 'flow/'
            elif isinstance(job, HRPLJob):
                fout_path += 'hrpl/'
            else:
                # Don't know how to name this output file
                continue

            fout_path += coupling_subdir

            new_copy_job = CopyJob(src=job.fout,
                                   dest=os.path.abspath(fout_path + '/' + job.fout))
            new_copy_job.depends_on = [job]
            copy_jobs.append(new_copy_job)
        
        ## Copy jobs for saved gauge files
        if gauge_dir is not None and hasattr(job, 'saveg') and job.saveg is not None:
            saveg_path = gauge_dir + vol_subdir + coupling_subdir
            new_copy_job = CopyJob(src=job.saveg,
                                   dest=os.path.abspath(saveg_path + '/' + job.saveg))
            new_copy_job.depends_on = [job]
            copy_jobs.append(new_copy_job)
        
        # Copy jobs for saved propagators (Spectro only)
        if prop_dir is not None and hasattr(job, 'savep') and job.savep is not None:
            savep_path = prop_dir 
            savep_path += ('prop4' if job.irrep == 'f' else 'prop6')
            savep_path += '_r{r0:g}/'.format(r0=job.r0)
            savep_path += vol_subdir + coupling_subdir
            
            new_copy_job = CopyJob(src=job.fout,
                                   dest=os.path.abspath(savep_path + '/' + job.savep),
                                   req_time=600)
            new_copy_job.depends_on = [job]
            copy_jobs.append(new_copy_job)
            
    return copy_jobs


#### Tools for adaptive nsteps for HMC jobs
#def get_last_N_hmc_jobs(hmc_job, N, look_through_branch_roots=False):
#    if N == 0:
#        return []
#
#    # Don't look through branch roots
#    if hmc_job.branch_root and not look_through_branch_roots:
#        return []
#
#    hmc_dependencies = filter(lambda j: isinstance(j, HMCJob), hmc_job.depends_on)
#    if len(hmc_dependencies) > 1:
#        raise Exception("Don't know how to handle an HMC run depending on multiple HMC runs")
#    elif len(hmc_dependencies) == 0:
#        return [] # At root of HMC tree
#    
#    return get_last_N_hmc_jobs(hmc_dependencies[0], N-1) + hmc_dependencies
#
#
#def use_adaptive_nsteps(job_pool, AR_from_last_N=2, min_AR=0.85, max_AR=0.9, die_AR=0.4, delta_nstep=1,
#                        adjust_branch_roots=False):
#    """Add NstepAdjustor jobs to pool to adaptively adjust nsteps for HMC jobs.
#    
#    If adjust_branch_roots is True, when a new HMC stream (branch) forks off from a running
#    stream (trunk), the current nsteps of the trunk will be applied to the branch.  If False,
#    whatever nsteps was specified by the user for the branch stream will be applied to the
#    branch root (first HMC job in the branch stream)."""
#
#    new_jobs = []
#
#    for job in job_pool:
#        if not isinstance(job, HMCJob):
#            continue
#
#        # New streams forking off start with the number of steps specified by the user
#        if job.branch_root and not adjust_branch_roots:
#            continue
#            
#        prev_hmc_jobs = get_last_N_hmc_jobs(job, AR_from_last_N, look_through_branch_roots=adjust_branch_roots)
#        if len(prev_hmc_jobs) < AR_from_last_N:
#            continue # Not deep enough in to stream to get enough AR data
#
#        adjust_job = NstepAdjustor(adjust_hmc_job=job,
#                                      examine_hmc_jobs=prev_hmc_jobs,
#                                      min_AR=min_AR, max_AR=max_AR, die_AR=die_AR, delta_nstep=delta_nstep)
#        job.depends_on.append(adjust_job) # HMC job needs to have its nsteps adjusted before running
#        adjust_job.depends_on = prev_hmc_jobs # Adjustor can't run until all the HMC jobs it needs to check ARs from have run
#        new_jobs.append(adjust_job)
#
#    return new_jobs + job_pool
        
    
### Dispatch class to build job dispatches
class dispatch(object):
    def __init__(self, job_pool, N_nodes,
                 taxi_time, taxi_name_generator, taxi_log_dir_for_name, max_taxis=None):

        self.job_pool = [j for j in job_pool] # copy list

        # Logistics to run
        self.N_nodes = N_nodes
        self.saved_forest_file = None
        self.workspace_dir = None
        self.launch_taxis = None
        
        self.taxi_name_generator = taxi_name_generator
        self.taxi_log_dir_for_name = taxi_log_dir_for_name
        self.taxi_time = taxi_time
        self.max_taxis = max_taxis

        # Error check        
        for j in job_pool:
            if j.req_time > self.taxi_time:
                raise Exception("Job {j} has req_time={rt} > taxi_time={tt}".format(j=j, rt=j.req_time, tt=self.taxi_time))
                
                
        # Compile
        self._find_trees()
        self._figure_out_taxi_spawns()
        self._compile()
        
                
    def _find_trees(self):
        ## Scaffolding
        # Give each task an identifier, reset dependents
        for jj, job in enumerate(self.job_pool):
            job._dependents = []

        # Let dependencies know they have a dependent
        for job in self.job_pool:
            for dependency in job.depends_on:
                dependency._dependents.append(job)
                
        ## Break apart jobs in to separate trees
        # First, find all roots
        self.trees = []
        for job in self.job_pool:
            if job.depends_on is None or len(job.depends_on) == 0:
                self.trees.append([job])

        ## Build out from roots
        # (NOT IMPLEMENTED) - If dependent has different number of nodes, make it a new tree
        # - I job is a trunk job and two dependents are trunk jobs, make one of them a new tree
        for tree in self.trees:
            for tree_job in tree:
                if not tree_job.trunk:
                    continue
                n_trunks_found = 0
                for d in tree_job._dependents:
                    # Count number of trunk tasks encountered in dependents, fork if this isn't the first
                    if d.trunk:
                        n_trunks_found += 1
                        if n_trunks_found > 1:
                            self.trees.append([d]) # Break branch off in to a new tree
                            continue
                    # Normal behavior: build on current tree
                    tree.append(d)
        
        
    def _figure_out_taxi_spawns(self):
        ## Figure out how many taxis this pool will need (up to optionally specified maximum)
        N_taxis = len(self.trees)  
        if self.max_taxis is not None and N_taxis > self.max_taxis:
            N_taxis = self.max_taxis
        self.taxi_names = [taxi_name for taxi_name in self.taxi_name_generator(N_taxis)]
        
        ## By construction, first job in each tree is the tree "root"
        # Roots with no dependencies are part of initial launch
        # Roots with dependencies are to be launched later, need to insert a spawn job
        self.launch_taxis = []
        for taxi_name, tree in zip(self.taxi_names, self.trees):
            root_job = tree[0]
            
            # Job is a launch root
            if root_job.depends_on is None or len(root_job.depends_on) == 0:
                self.launch_taxis.append(taxi_name)
                continue
                
            # Taxi needs to be launched during stream
            spawn_task = SpawnJob(taxi_name=taxi_name, taxi_time=self.taxi_time,
                                  taxi_nodes=self.N_nodes, log_dir=self.taxi_log_dir_for_name(taxi_name),
                                  depends_on=root_job.depends_on)
            self.job_pool.insert(0, spawn_task)

        ## Put respawn job at end of pool for resubmitting behavior
        respawn_task = RespawnJob()
        self.job_pool.append(respawn_task)
        
        
    def _compile(self):
        # Give each job an integer id
        for jj, job in enumerate(self.job_pool):
            job.job_id = jj
            
        # Tell all jobs to compile themselves
        for job in self.job_pool:
            job.compile()
            
        self.compiled_pool = [job.compiled for job in self.job_pool]
        
    def save_forest_db(self, forest_file, overwrite=False):
        forest_file = os.path.abspath(forest_file)
        self.saved_forest_file = forest_file
        print "Saving job forest to file", forest_file
        
        if os.path.exists(forest_file) and not overwrite:
            raise Exception("File {forest_file} already exists. Call save_forest_db with overwrite=True.".format(forest_file=forest_file))
            
        ## Make SQLite forest DB file
        # Open connection to forest DB
        conn = sqlite3.connect(forest_file)
        # Make tables
        print "Making tables"
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id integer PRIMARY KEY,
                    task_type text,
                    depends_on text,
                    status text,
                    for_taxi text,
                    by_taxi test,
                    req_time integer DEFAULT 0,
                    run_time real DEFAULT -1,
                    task_args text
                )""")
            conn.execute("DELETE FROM tasks")
            
            conn.execute("""
                    CREATE TABLE IF NOT EXISTS priority (
                        id integer PRIMARY KEY,
                        taxi text,
                        list text,
                        CONSTRAINT priority_taxi_unique UNIQUE (taxi) 
                    )""")
            conn.execute("DELETE FROM priority")
            
            conn.execute("""
                    CREATE TABLE IF NOT EXISTS taxis (
                        id integer PRIMARY KEY,
                        taxi_name text,
                        taxi_time real,
                        taxi_forest text,
                        taxi_dir text,
                        nodes integer,
                        is_launch_taxi integer,
                        CONSTRAINT taxis_unique UNIQUE (taxi_name)
                    )
            """)
            conn.execute("DELETE FROM taxis")
            
        # Populate priority list table
        print "Populating priority list"
        with conn:
            priority_list = [job['id'] for job in self.compiled_pool]
            conn.execute("""INSERT OR REPLACE INTO priority (taxi, list)
                            VALUES (?, ?)""",
                        ('all', json.dumps(priority_list)))
            
        # Populate task table
        print "Populating task table"
        with conn:
            for job in self.compiled_pool:
                conn.execute("""
                    INSERT OR REPLACE INTO tasks (id, task_type, depends_on, status, req_time, task_args)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (job['id'], job['task_type'],
                     json.dumps(job['depends_on']),
                     job['status'], job['req_time'],
                     json.dumps(job['task_args']) if job.has_key('task_args') else None))
                
        # Populate taxis table
        print "Populating taxis table"
        with conn:
            for taxi_name in self.taxi_names:
                conn.execute("""
                    INSERT OR REPLACE INTO taxis (taxi_name, taxi_time, taxi_forest, taxi_dir, nodes, is_launch_taxi)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (taxi_name, self.taxi_time, forest_file,
                      self.taxi_log_dir_for_name(taxi_name), self.N_nodes,
                      1 if taxi_name in self.launch_taxis else 0))
        
            
    def prepare_workspace(self, workspace_dir):
        ## Dig out directories to work in
        workspace_dir = os.path.abspath(workspace_dir)
        self.workspace_dir = workspace_dir
        mkdir_p(workspace_dir)
            

    def launch(self, taxi_launcher, taxi_shell_script):
        ## Make sure we're set up and ready to go
        if self.workspace_dir is None:
            print "Must call .prepare_workspace(workspace_dir != None) before launch."
            return
        if self.saved_forest_file is None:
            print "Must call .save_forest_db(forest_file) before launch."
            return
        if self.launch_taxis is None or len(self.launch_taxis) == 0:
            print "No launch taxis; something went wrong with compile"
            return
            
        ## Dig out log directories for taxis (also used to keep track of what taxi names are unavailable)
        for taxi_name in self.taxi_names:
            mkdir_p(self.taxi_log_dir_for_name(taxi_name))
        
        ## Launch all of the launch_taxis using the provided function
        for taxi_name in self.launch_taxis:
            taxi_launcher(taxi_name=taxi_name, taxi_forest=self.saved_forest_file, home_dir=self.workspace_dir,
                          taxi_dir=self.taxi_log_dir_for_name(taxi_name),
                          taxi_time=self.taxi_time, taxi_nodes=self.N_nodes,
                          taxi_shell_script=taxi_shell_script)

            
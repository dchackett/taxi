#!/usr/bin/env python
import os

import taxi
import taxi.pool
import taxi.dispatcher
import taxi.mcmc as mcmc
import taxi.apps.mrep_milc.hmc_singlerep as hmc
import taxi.apps.mrep_milc.flow as flow
import taxi.apps.mrep_milc.spectro as spectro

# Plug in binary locations
hmc.SingleRepHMCTask.binary = '/nfs/beowulf03/dchackett/mrep/bin/su4_mrep_hmc'
flow.FlowTask.binary = '/nfs/beowulf03/dchackett/mrep/bin/su4_wf_mpi'
spec6_binary = '/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg' # Plugged in later

# Plug in desired file-naming conventions
flow.FlowTask.loadg.conventions = "{loadg_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"
flow.FlowTask.fout.conventions = "flow_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"
spectro.SpectroTask.loadg.conventions = "{loadg_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"
spectro.SpectroTask.fout.conventions = "{fout_prefix}_r{r0:g}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"

# Specify paths to Dispatch and Pools DBS
base_path = os.path.abspath("./taxi-test")
pool_db_path = base_path + "/test-pool.sqlite"
dispatch_db_path = base_path + "/test-disp.sqlite"


# Taxis will import this file, so everything except for imports and
# relevant declarations need to go in to a __main__ block so the job isn't
# repeatedly respecified
if __name__ == '__main__':
    ## Set up HMC streams
    # First stream, start from fresh
    seed_stream = mcmc.make_config_generator_stream(
        config_generator_class=hmc.SingleRepHMCTask,
        streamseed=1,
        N=4,
        Ns=4, Nt=4,
        beta=7.75, kappa=0.125, irrep='as2',
        nsteps1=10,
        n_traj=2, n_safe=2, minAR=0,
        label='1',
        starter=None,
        req_time=240,
    )
    
    # Second stream forks from the first
    fork_stream = mcmc.make_config_generator_stream(
        config_generator_class=hmc.SingleRepHMCTask,
        streamseed=1,
        N=2,
        Ns=4, Nt=4,
        beta=7.76, kappa=0.125, irrep='f',
        nsteps1=10,
        n_traj=2, n_safe=2, minAR=0,
        label='1',
        starter=seed_stream[1],
        req_time=240,
    )
    
    hmc_pool = seed_stream + fork_stream
    
    ## Add Wilson flow tasks for both streams
    flow_pool = mcmc.measure_on_config_generators(
        config_measurement_class=flow.FlowTask,
        measure_on=hmc_pool,
        req_time=60,
        tmax=1,
        start_at_traj=4
    )
    
    ## Add A2 spectroscopy tasks for both streams    
    spec6_pool = mcmc.measure_on_config_generators(
        config_measurement_class=spectro.SpectroTask,
        measure_on=hmc_pool,
        req_time=60,
        start_at_traj=4,
        r0=6.,
        kappa='kappa', # Tells task to steal kappa from SinglerepHMCTask
        binary=spec6_binary, fout_prefix='tspec_a2'
    )
    
    task_pool = hmc_pool + flow_pool + spec6_pool
    
    
    ## Set up pool and feed it taxis
    pool_name = "srep_test"
    my_pool = taxi.pool.SQLitePool(
        db_path=pool_db_path, 
        pool_name=pool_name, 
        work_dir=(base_path + "/work/"),
        log_dir=(base_path + "/log/")
    )
    
    ## Setup dispatcher
    my_disp = taxi.dispatcher.SQLiteDispatcher(db_path=dispatch_db_path)
    
    ## Initialize task pool with the dispatch
    with my_disp:
        my_disp.initialize_new_task_pool(task_pool)
    
    # Create taxi(s) to run the job
    taxi_list = []
    for i in range(2):
        taxi_list.append(taxi.Taxi(
            time_limit=10*60,
            cores=8,
            nodes=1
        ))
            
    ## Register taxis and launch!
    with my_pool:
        for my_taxi in taxi_list:
            my_pool.register_taxi(my_taxi)
            my_disp.register_taxi(my_taxi, my_pool)
    
        my_pool.spawn_idle_taxis(dispatcher=my_disp)
    
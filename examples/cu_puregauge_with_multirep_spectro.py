#!/usr/bin/env python
import os

import taxi
import taxi.pool
import taxi.dispatcher
import taxi.mcmc as mcmc
import taxi.apps.mrep_milc.flow as flow
import taxi.apps.mrep_milc.pure_gauge_ora as pg_ora
import taxi.apps.mrep_milc.spectro as spectro


# Plug in desired file-naming conventions
flow.FlowTask.loadg.conventions = "{loadg_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"
spectro.SpectroTask.loadg.conventions = "{fout_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"
flow.FlowTask.fout.conventions = "flow_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"
spectro.SpectroTask.fout.conventions = "{fout_prefix}_{irrep}_r{r0:g}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"


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
        config_generator_class=pg_ora.PureGaugeORATask,
        streamseed=1,
        N=10,
        Ns=4,
        Nt=4,
        beta=7.75,
        label='1',
        starter=None,
        req_time=240,
    )
    
    # Second stream forks from the first
    fork_stream = mcmc.make_config_generator_stream(
        config_generator_class=pg_ora.PureGaugeORATask,
        streamseed=2,
        N=5,
        Ns=4,
        Nt=4,
        beta=7.76,
        label='1',
        starter=seed_stream[4],
        req_time=240,
    )
    
    hmc_pool = seed_stream + fork_stream
    
    ## Add Wilson flow tasks for both streams
    flow_pool = mcmc.measure_on_config_generators(
        config_measurement_class=flow.FlowTask,
        measure_on=hmc_pool,
        req_time=60,
        tmax=1,
        start_at_traj=200
    )
    
    ## Add F and A2 (quenched) spectroscopy tasks for both streams
    spec4_pool = mcmc.measure_on_config_generators(
        config_measurement_class=spectro.SpectroTask,
        measure_on=hmc_pool,
        req_time=60,
        start_at_traj=200,
        r0=6., kappa=0.125, irrep='f'
    )
    
    spec6_pool = mcmc.measure_on_config_generators(
        config_measurement_class=spectro.SpectroTask,
        measure_on=hmc_pool,
        req_time=60,
        start_at_traj=200,
        r0=6., kappa=0.125, irrep='a2'
    )
    
    task_pool = hmc_pool + flow_pool + spec4_pool + spec6_pool
    
    ## Set up pool and feed it taxis
    pool_name = "pg_test"
    my_pool = taxi.pool.SQLitePool(
        db_path=pool_db_path, 
        pool_name=pool_name, 
        work_dir=(base_path + "/pool/"),
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
    
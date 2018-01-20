#!/usr/bin/env python
import os

import taxi
import taxi.pool
import taxi.dispatcher
import taxi.mcmc as mcmc
import taxi.apps.mrep_milc.spectro as spectro
import taxi.apps.mrep_milc.mrep_fncs as mrep_fncs


# Plug in desired file-naming conventions
spectro.SpectroTask.loadg.conventions = "{loadg_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"
spectro.SpectroTask.fout.conventions = "{fout_prefix}_{irrep}_r{r0:g}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}"

# Specify paths to Dispatch and Pools DBS
base_path = os.path.abspath("./taxi-test")
pool_db_path = base_path + "/test-pool.sqlite"
dispatch_db_path = base_path + "/test-disp.sqlite"


# Taxis will import this file, so everything except for imports and
# relevant declarations need to go in to a __main__ block so the job isn't
# repeatedly respecified
if __name__ == '__main__':
    with open('sample_data/gaugefiles') as f:
        gauge_files = f.readlines()
        gauge_files = map(os.path.abspath, gauge_files) # Get absolute paths to gaugefiles
    
    ## Add F and A2 (quenched) spectroscopy tasks for both streams
    spec4_pool = mcmc.measure_on_files(
        config_measurement_class=spectro.SpectroTask,
        filenames=gauge_files,
        req_time=600,
        start_at_traj=10,
        r0=6., irrep='f'
    )
    
    spec6_pool = mcmc.measure_on_files(
        config_measurement_class=spectro.SpectroTask,
        filenames=gauge_files,
        req_time=600,
        start_at_traj=10,
        r0=6., irrep='a2'
    )
    
    task_pool = spec4_pool + spec6_pool
    
    task_pool += mrep_fncs.copy_tasks_for_multirep_outputs(task_pool,
                    out_dir=base_path+'/data/',
                    gauge_dir=base_path+'/gauge/')
    
    ## Set up pool and feed it taxis
    pool_name = "spec"
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
    
    # Create taxi(s) to run the tasks
    taxi_list = []
    for i in range(20):
        taxi_list.append(taxi.Taxi(
            time_limit=10*3600,
            cores=8,
            nodes=1
        ))
            
    ## Register taxis and launch!
    with my_pool:
        for my_taxi in taxi_list:
            my_pool.register_taxi(my_taxi)
            my_disp.register_taxi(my_taxi, my_pool)
    
        my_pool.spawn_idle_taxis(dispatcher=my_disp)
    

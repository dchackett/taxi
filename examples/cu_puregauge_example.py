#!/usr/bin/env python
import os

import taxi
import taxi.pool
import taxi.dispatcher
import taxi.mcmc as mcmc
import taxi.apps.mrep_milc.flow as flow
import taxi.apps.mrep_milc.pure_gauge_ora as pg_ora

import taxi.local.local_queue as local_queue

## Set up HMC streams
# First stream, start from fresh
seed_stream = mcmc.make_config_generator_stream(
    config_generator_class=pg_ora.PureGaugeORAJob,
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
    config_generator_class=pg_ora.PureGaugeORAJob,
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
    config_measurement_class=flow.FlowJob,
    config_generators=hmc_pool,
    req_time=60,
    tmax=1,
    start_at_traj=200
)

job_pool = hmc_pool + flow_pool

## Create taxis; let's have two identical ones
taxi_list = []
pool_name = "pg_test"
for i in range(1):
    taxi_list.append(taxi.Taxi(
        name="pg_test{}".format(i), 
        pool_name=pool_name,
        time_limit=5*60,
        cores=8,
        nodes=1
    ))

## Set up pool and dispatch
## TODO: this feels like it could be condensed into a single convenience function
base_path = os.path.abspath("./taxi-test")
pool_path = base_path + "/test-pool.sqlite"
pool_wd = base_path + "/pool/"
pool_ld = base_path + "/pool/log"

disp_path = base_path + "/test-disp.sqlite"

my_pool = taxi.pool.SQLitePool(pool_path, pool_name, pool_wd, pool_ld)
my_disp = taxi.dispatcher.SQLiteDispatcher(disp_path)

my_queue = local_queue.LocalQueue()

## Initialize task pool with the dispatch
with my_disp:
    my_disp.initialize_new_job_pool(job_pool)

## Register taxis and launch!
with my_pool:
    for my_taxi in taxi_list:
        my_pool.register_taxi(my_taxi)
        my_disp.register_taxi(my_taxi, my_pool)

    my_pool.spawn_idle_taxis(my_queue)


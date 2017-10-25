#!/usr/bin/env python

# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 13:00:44 2017

@author: wijay
"""

import os

import taxi
import taxi.pool
import taxi.dispatcher
import taxi.mcmc as mcmc
import taxi.apps.mrep_milc.mrep_fncs as mrep_fncs
import taxi.apps.mrep_milc.flow as flow
import taxi.apps.mrep_milc.pure_gauge_ora as pg_ora
import taxi.apps.mrep_milc.spectro as spectro

import taxi.local.local_queue as local_queue

# Plug in desired file-naming conventions
#flow.FlowJob.loadg_filename_convention = mrep_fncs.MrepFnConvention
#flow.FlowJob.fout_filename_convention = mrep_fncs.MrepFnConvention
#spectro.SpectroTask.loadg_filename_convention = mrep_fncs.MrepFnConvention
#spectro.SpectroTask.fout_filename_convention = mrep_fncs.MrepSpectroFnConvention


###########################################
# Plug in desired file-naming conventions #
###########################################

spectro.SpectroTask.loadg_filename_convention = mrep_fncs.MrepGaugeWijayFnConvention
spectro.SpectroTask.fout_filename_convention = mrep_fncs.MrepSpectroWijayFnConvention
spectro.SpectroTask.prop_filename_convention = mrep_fncs.MrepPropWijayFnConvention

# Specify paths to Dispatch and Pools DBS
base_path = os.path.abspath("./taxi-test")
pool_db_path = base_path + "/test-pool.sqlite"
dispatch_db_path = base_path + "/test-disp.sqlite"

def main():

    #################
    # Control panel #
    #################
    
    ntaxis = 10
    ## Specify which settings to use for spectroscopy
    spec_settings = {'r0':12.,
                     'irrep':'as'}
    
    ## Specify bare params
    ensembles = [
        {'beta':'7.22', 'k4':'0.13152', 'k6':'0.13423'},
        {'beta':'7.24', 'k4':'0.13172', 'k6':'0.13396'},           
    ]
        
    ## Defaults
    for ens in ensembles:
        ens['Ns'] = ens.get('Ns', 16)
        ens['Nt'] = ens.get('Nt', 32)
        ens['series'] = ens.get('series', 1)
    
    ## Fetch list of filenames
    gnames = []
    pnames = []
    for ens in ensembles:
        gauge_dir = _get_gauge_dir(**ens)
        prop_dir = _get_prop_dir(r0=spec_settings['r0'],
                                 irrep=spec_settings['irrep'],
                                 **ens)

        for fname in os.listdir(gauge_dir):
            if fname.startswith('cfg'):
                ## Get full path for gauge file
                gname = os.path.join(gauge_dir, fname)
                gnames.append(gname)
                
                ## Get parameters for constructing prop filename
                params = mrep_fncs.MrepGaugeWijayFnConvention.read(fname)
                params['prefix'] = 'prop'
                for k,v in spec_settings:
                    params[k] = v

                ## Get full path for prop files
                pname = mrep_fncs.MrepPropWijayFnConvention.write(params)
                pname = os.path.join(prop_idr, pname)
                pnames.append(pname)
                
    ##########################
    # Specify tasks for taxi # 
    ##########################
    
    spec6_pool = []
    for gname, pname in zip(gnames, pnames):
        spec6_pool += mcmc.measure_on_files(
                        config_measurement_class=spectro.SpectroTask,
                        filenames=[gname],
                        req_time=12*60*60, # 12 hours
                        savep=pname,
                        **spec_settings
                        )

    ##################################
    # Set up job pool, feed it taxis #
    ##################################

    job_pool = spec6_pool
    pool_name = 'wijay_spec6_test'
    my_pool = taxi.pool.SQLitePool(
                db_path=pool_db_path,
                pool_name=pool_name,
                work_dir=(os.path.join(base_path,'pool')),
                log_dir=(os.path.join(base_path,'log'))
                )        

    ## Setup dispatcher
    my_disp = taxi.dispatcher.SQLiteDispatcher(db_path=dispatch_db_path)
    
    ## Initialize task pool with the dispatch
    with my_disp:
        my_disp.initialize_new_job_pool(job_pool)
    
    # Create taxi(s) to run the job
    taxi_list = []
    for i in range(ntaxis):
        taxi_list.append(taxi.Taxi(
            time_limit=13*60*60, # 13 hrs, buffer so that taxis run
            cores=8,
            nodes=1
        ))
            
    ## Register taxis and launch!
    with my_pool:
        for my_taxi in taxi_list:
            my_pool.register_taxi(my_taxi)
            my_disp.register_taxi(my_taxi, my_pool)
    
        my_pool.spawn_idle_taxis(dispatcher=my_disp)
    
def _get_gauge_dir(Ns, Nt, beta, k4, k6, series, irrep):
    
    params = {'Ns':Ns,
              'Nt':Nt,
              'beta':beta,
              'k4',k4,
              'k6',k6,
              'series':series}    
    base_dir = '/nfs/beowulf03/wijay/Run_SU4_Nf2_Nas2_{Ns}{Nt}_FNAL'.format(**params)
    gauge_dir = 'Gauge{Ns}{Nt}_b{beta}_kf{k4}_kas{k6}_{series}'.format(**params)
    os.path.join(base_dir, gauge_dir)    

def _get_prop_dir(Ns, Nt, beta, k4, k6, series, irrep):
    
    dir1 = '/nfs/beowulf03/wijay/Run_SU4_Nf2_Nas2_1632_FNAL'
    dir2 = '/nfs/beowulf03/wijay/Run_SU4_Nf2_Nas2_1632_FNAL/Corrected'
    
    locations = {
        ## Following ensembles live in dir1
        ('16', '32', '7.22', '0.13152', '0.13423', '1', 'as'):dir1,
        ('16', '32', '7.22', '0.13152', '0.13423', '1', 'f'):dir1,
        ('16', '32', '7.24', '0.13172', '0.13396', '1', 'as'):dir1,
        ('16', '32', '7.24', '0.13172', '0.13396', '1', 'f'):dir1,
        ('16', '32', '7.25', '0.13095', '0.13418', '1', 'as'):dir1,
        ('16', '32', '7.25', '0.13095', '0.13418', '1', 'f'):dir1,
        ('16', '32', '7.25', '0.13147', '0.13395', '1', 'as'):dir1,
        ('16', '32', '7.25', '0.13147', '0.13395', '1', 'f'):dir1,
        ('16', '32', '7.30', '0.13117', '0.13363', '1', 'as'):dir1,
        ('16', '32', '7.30', '0.13117', '0.13363', '1', 'f'):dir1,
        ('16', '32', '7.30', '0.13162', '0.13340', '1', 'as'):dir1,
        ('16', '32', '7.30', '0.13162', '0.13340', '1', 'f'):dir1,
        ('16', '32', '7.55', '0.1290', '0.1325', '1', 'as'):dir1,
        ('16', '32', '7.55', '0.1290', '0.1325', '1', 'f'):dir1,
        ## Following ensembles live in dir2
        ('16', '32', '7.20', '0.13172', '0.13425', '1', 'as'):dir2,
        ('16', '32', '7.20', '0.13172', '0.13425', '1', 'f'):dir2,
        ('16', '32', '7.4', '0.131', '0.13', '1', 'as'):dir2,
        ('16', '32', '7.4', '0.131', '0.13', '1', 'f'):dir2,
        ('16', '32', '7.55', '0.1290', '0.1325', '1', 'as'):dir2,
        ('16', '32', '7.55', '0.1290', '0.1325', '1', 'f'):dir2,
        ('16', '32', '7.55', '0.1300', '0.1325', '1', 'as'):dir2,
        ('16', '32', '7.55', '0.1300', '0.1325', '1', 'f'):dir2,
        ('16', '32', '7.55', '0.1310', '0.1325', '1', 'as'):dir2,
        ('16', '32', '7.55', '0.1310', '0.1325', '1', 'f'):dir2,
        ('16', '32', '7.65', '0.1280', '0.1310', '1', 'as'):dir2,
        ('16', '32', '7.65', '0.1280', '0.1310', '1', 'f'):dir2,
        ('16', '32', '7.65', '0.1280', '0.1325', '1', 'as'):dir2,
        ('16', '32', '7.65', '0.1280', '0.1325', '1', 'f'):dir2,
        ('16', '32', '7.65', '0.1285', '0.1325', '1', 'as'):dir2,
        ('16', '32', '7.65', '0.1285', '0.1325', '1', 'f'):dir2,
        ('16', '32', '7.65', '0.1290', '0.1308', '1', 'as'):dir2,
        ('16', '32', '7.65', '0.1290', '0.1308', '1', 'f'):dir2,
        ('16', '32', '7.65', '0.1290', '0.1325', '1', 'as'):dir2,
        ('16', '32', '7.65', '0.1290', '0.1325', '1', 'f'):dir2,
        ('16', '32', '7.65', '0.1300', '0.1310', '1', 'as'):dir2,
        ('16', '32', '7.65', '0.1300', '0.1310', '1', 'f'):dir2,
        ('16', '32', '7.65', '0.1300', '0.1320', '1', 'as'):dir2,
        ('16', '32', '7.65', '0.1300', '0.1320', '1', 'f'):dir2,
        ('16', '32', '7.75', '0.1280', '0.1310', '1', 'as'):dir2,
        ('16', '32', '7.75', '0.1280', '0.1310', '1', 'f'):dir2,
        ('16', '32', '7.75', '0.1290', '0.1308', '1', 'as'):dir2,
        ('16', '32', '7.75', '0.1290', '0.1308', '1', 'f'):dir2,
        ('16', '32', '7.75', '0.1295', '0.1315', '1', 'as'):dir2,
        ('16', '32', '7.75', '0.1295', '0.1315', '1', 'f'):dir2,
        ('16', '32', '7.75', '0.1295', '0.1320', '1', 'as'):dir2,
        ('16', '32', '7.75', '0.1295', '0.1320', '1', 'f'):dir2,
        ('16', '32', '7.75', '0.1298', '0.1317', '1', 'as'):dir2,
        ('16', '32', '7.75', '0.1298', '0.1317', '1', 'f'):dir2,
        ('16', '32', '7.75', '0.1300', '0.1315', '1', 'as'):dir2,
        ('16', '32', '7.75', '0.1300', '0.1315', '1', 'f'):dir2,
        ('16', '32', '7.75', '0.1300', '0.1320', '1', 'as'):dir2,
        ('16', '32', '7.75', '0.1300', '0.1320', '1', 'f'):dir2,
        ('16', '32', '7.75', '0.13', '0.1295', '1', 'as'):dir2,
        ('16', '32', '7.75', '0.13', '0.1295', '1', 'f'):dir2,
        ('16', '32', '7.85', '0.1290', '0.1308', '1', 'as'):dir2,
        ('16', '32', '7.85', '0.1290', '0.1308', '1', 'f'):dir2,
    }

    key = (Ns, Nt, beta, k4, k6, series, irrep)
    return locations.get(key, None)

if __name__ == '__main__':
    main()    

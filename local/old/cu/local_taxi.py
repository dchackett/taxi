import os
import subprocess
import time

### Queue interaction utilities
def taxi_in_queue(taxi_name, suppress_output=False):
    """Anti-thrashing utility function.  Checks if a taxi with this name is already
    in the queue, so we don't submit another one."""
    #found_taxi = (os.system("qstat -j {taxi_name}".format(taxi_name=taxi_name)) >> 8) == 0 # If the taxi isn't in the queue, return code is an error
    if suppress_output:
        found_taxi = subprocess.call("qstat -j {taxi_name}".format(taxi_name=taxi_name), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) == 0
    else:
        found_taxi = subprocess.call("qstat -j {taxi_name}".format(taxi_name=taxi_name), shell=True) == 0
    
    return found_taxi

    
def taxi_launcher(taxi_name, taxi_forest, home_dir, taxi_dir, taxi_time, taxi_nodes, taxi_shell_script):
    if taxi_nodes is not None and taxi_nodes > 1:
        raise Exception("Can only run single-node jobs on beowulf")
        
    # Arguments to SGE (Note, don't specify nodes -- always 1)
    taxi_call = "Qsub -e -G beowulf "
    taxi_call += " -N {taxi_name} ".format(taxi_name=taxi_name)
    
    # Logs go in taxi dir, like dtaxi/taxi-name_1241234.log
    logfn = "{taxi_dir}/{taxi_name}_{subtime}".format(taxi_dir=taxi_dir, taxi_name=taxi_name,
                                                          subtime=time.strftime("%Y%b%d-%H%M"))
    taxi_call += " -o {logfn} ".format(logfn=logfn)
    
    ## Call taxi wrapper bash script
    taxi_call += " " + taxi_shell_script + " "
    
    ## Taxi arguments
    # NOTE: Bash script must add on "--name" and --nodes" and "--cpus ..." !
    taxi_call += " --dtaxi {taxi_dir} --time {taxi_time} ".format(taxi_dir=taxi_dir, taxi_time=taxi_time)
    taxi_call += " --forest {taxi_forest} --dhome {dhome} ".format(taxi_forest=taxi_forest, dhome=home_dir)

    # Submit the job, catch stdout and stderr
    batch_out, batch_err = subprocess.Popen(taxi_call, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()

    print batch_out
    if batch_err != '':
        print batch_err
        print "spawn: Something went wrong with batch submission"
        print "taxi_call:", taxi_call
        return False
    return True

#def spawn_new_taxi(task_args, parg):
#    """Execute a spawn task.  Builds a command-line call to SLURM's sbatch that will
#    submit a taxi job to the queue.
#    
#    Returns: True if sbatch submission seems to have worked correctly."""
#    
#    # Don't spawn duplicate taxis, but allow respawning
#    if task_args['taxi_name'] != parg.name and taxi_in_queue(task_args['taxi_name']):
#        print "spawn: Taxi {taxi_name} already in queue"
#        return False
#    
#    taxi_call = "Qsub -e -G beowulf "
#    taxi_call += " -N {taxi_name} ".format(**task_args)
#
#    # Logs go in taxi dir, like dtaxi/taxi-name_1241234.log
#    logfn = "{taxi_dir}/{taxi_name}_{subtime}.log".format(subtime=time.strftime('%Y%m%d%H%M'), **task_args)
#    taxi_call += " -o {logfn} ".format(logfn=logfn)
#    
#    ## Call taxi wrapper bash script
#    taxi_call += " " + parg.shell + " "
#    
#    ## Taxi arguments
#    # NOTE: Bash script must add on "--name" and --nodes" and "--cpus ..." !
#    taxi_call += " --dtaxi {taxi_dir} --time {taxi_time} ".format(**task_args)
#   
#    # Any taxi spawned by a taxi should have same work dir (all output files kept in the same place)
#    taxi_call += " --forest {taxi_forest} --dhome {dhome} ".format(dhome=parg.dhome, taxi_forest=parg.forest)
#
#    # Submit the job, catch stdout and stderr
#    batch_out, batch_err = subprocess.Popen(taxi_call, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()
#
#    print batch_out    
#    if batch_err != '':
#        print batch_err
#        print "spawn: Something went wrong with batch submission"
#        print "taxi_call:", taxi_call
#        return False
#    return True

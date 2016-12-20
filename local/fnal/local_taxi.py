import os
import subprocess
import time


### Queue interaction utilities
def taxi_in_queue(taxi_name):
    """Anti-thrashing utility function.  Checks if a taxi with this name is already
    in the queue, so we don't submit another one."""
    found_taxi = (os.system("qstat | grep -qc {taxi_name}".format(taxi_name=taxi_name))) == 0 # If the taxi isn't in the queue, return code is an error
    return found_taxi

    
def taxi_launcher(taxi_name, taxi_forest, home_dir, taxi_dir, taxi_time, taxi_nodes, taxi_shell_script):
    ## Time logistics
    bash_time = taxi_time
    
    # Translate taxi_time from seconds to hh:mm:ss format
    # Include a buffer between bash script requested time and taxi time budget 
    bash_time += 300 # HARDCODE: 5 minute buffer
    hours = int(bash_time / 3600)
    minutes = int(bash_time - 3600*hours)/60
    seconds = int(bash_time - 3600*hours - 60*minutes)
    time_str = "{hours:02d}:{minutes:02d}:{seconds:02d}".format(hours=hours, minutes=minutes, seconds=seconds)
    
    ## Arguments for PBS
    taxi_call = "/usr/local/pbs/bin/qsub "
    taxi_call += " -N {taxi_name} ".format(taxi_name=taxi_name)
    taxi_call += " -l walltime={walltime} ".format(walltime=time_str)
    taxi_call += " -l nodes={taxi_nodes} ".format(taxi_nodes=taxi_nodes)
    
    # Logs go in taxi dir, like dtaxi/taxi-name_1241234.log
    # PBS should replace $PBS_JOBID in -o to the job id, but it doesn't.
    #logfn = "{taxi_dir}/{taxi_name}_$PBS_JOBID.log".format(taxi_dir=taxi_dir, taxi_name=taxi_name)
    logfn = "{taxi_dir}/{taxi_name}_{subtime}.log".format(taxi_dir=taxi_dir, taxi_name=taxi_name, subtime=time.strftime('%Y%m%d%H%M'))
    taxi_call += " -o {logfn} ".format(logfn=logfn)

    ## Call taxi wrapper bash script
    taxi_call += " " + taxi_shell_script + " "
    
    ## Taxi arguments
    # NOTE: Bash script must add on "--name" and --nodes" and "--cpus ..." !
    taxi_args = " --dtaxi {taxi_dir} --time {taxi_time} ".format(taxi_dir=taxi_dir, taxi_time=taxi_time)
    taxi_args += " --forest {taxi_forest} --dhome {dhome} ".format(taxi_forest=taxi_forest, dhome=home_dir)
    
    ### PBS-specific -- command line args to script must be passed as string to -F
    if '"' in taxi_args or "'" in taxi_args:
        print "LAUNCH FAILED: Taxi launcher tried to use command line arguments with quotes in them."
        print "taxi_args:", taxi_args
        return False
    taxi_call += """ -F \\"{taxi_args}\\" """.format(taxi_args=taxi_args)

    ## FNAL oddity -- Apparently on worker nodes, can only qsub via restricted shell
    taxi_call = """/usr/bin/rsh bc1p "{taxi_call}" """.format(taxi_call=taxi_call)
    
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
#    """Execute a spawn task.  Builds a command-line call to PBS's qsub that will
#    submit a taxi job to the queue.
#    
#    Returns: True if PBS submission seems to have worked correctly."""
#    
#    # Don't spawn duplicate taxis, but allow respawning
#    if task_args['taxi_name'] != parg.name and taxi_in_queue(task_args['taxi_name']):
#        print "spawn: Taxi {taxi_name} already in queue"
#        return False
#    
#    ## Time logistics
#    bash_time = task_args['taxi_time']
#    
#    # Translate taxi_time from seconds to hh:mm:ss format
#    # Include a buffer between bash script requested time and taxi time budget 
#    bash_time += 300 # HARDCODE: 5 minute buffer
#    hours = int(bash_time / 3600)
#    minutes = int(bash_time - 3600*hours)/60
#    seconds = int(bash_time - 3600*hours - 60*minutes)
#    time_str = "{hours:02d}:{minutes:02d}:{seconds:02d}".format(hours=hours, minutes=minutes, seconds=seconds)
#        
#    taxi_call = "/usr/local/pbs/bin/qsub "
#    taxi_call += " -N {taxi_name} ".format(**task_args)
#
#    # Logs go in taxi dir, like dtaxi/taxi-name_1241234.log
#    logfn = "{taxi_dir}/{taxi_name}_$PBS_JOBID.log".format(**task_args)
#    taxi_call += " -o {logfn} ".format(logfn=logfn)
#
#    # Job size    
#    taxi_call += " -l walltime={walltime} ".format(walltime=time_str)
#    taxi_call += " -l nodes={taxi_nodes} ".format(**task_args)
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

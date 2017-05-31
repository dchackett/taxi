import subprocess

### Queue interaction utilities
def taxi_in_queue(taxi_name, suppress_output=False):
    """Anti-thrashing utility function.  Checks if a taxi with this name is already
    in the queue, so we don't submit another one."""
    ntaxis, err = subprocess.Popen("squeue --name='%s' | grep -c '%s'"%(taxi_name, taxi_name),
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()
    return int(ntaxis) > 0


def taxi_launcher(taxi_name, taxi_forest, home_dir, taxi_dir, taxi_time, taxi_nodes, taxi_shell_script):   
    ## Time logistics
    bash_time = taxi_time
    
    # Translate taxi_time from seconds to hh:mm:ss format
    # Include a buffer between bash script requested time and taxi time budget 
    bash_time += 300 # HARDCODE: 5 minute buffer
    hours = int(bash_time / 3600)
    minutes = int(bash_time - 3600*hours)/60
    seconds = int(bash_time - 3600*hours - 60*minutes)
    janus_time_str = "{hours:02d}:{minutes:02d}:{seconds:02d}".format(hours=hours, minutes=minutes, seconds=seconds)
    
    ## Arguments to SLURM
    taxi_call  = "sbatch "
    taxi_call += " --nodes {taxi_nodes} --job-name {taxi_name} ".format(taxi_nodes=taxi_nodes, taxi_name=taxi_name)
    taxi_call += " --time {time_str} ".format(time_str=janus_time_str)
    if taxi_nodes == 1:
        # Janus has a special reservation for jobs that only require one node, use it to reduce wait times
        taxi_call += " --reservation janus-serial "
    
    # Logs go in taxi dir, like dtaxi/taxi-name_1241234.log
    logfn = "{taxi_dir}/{taxi_name}_%j.log".format(taxi_dir=taxi_dir, taxi_name=taxi_name)
    taxi_call += " --open-mode append --output {logfn} ".format(logfn=logfn) # SLURM
    
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
#        print "spawn: Taxi {taxi_name} already in queue".format(taxi_name = task_args['taxi_name'])
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
#    janus_time_str = "{hours:02d}:{minutes:02d}:{seconds:02d}".format(hours=hours, minutes=minutes, seconds=seconds)
#    
#    ## Arguments to SLURM
#    taxi_call  = "sbatch "
#    taxi_call += " --nodes {taxi_nodes} --job-name {taxi_name} ".format(**task_args)
#    taxi_call += " --time {time_str} ".format(time_str=janus_time_str)
#    if task_args['taxi_nodes'] == 1:
#        # Janus has a special reservation for jobs that only require one node, use it to reduce wait times
#        taxi_call += " --reservation janus-serial "
#
#    # Logs go in taxi dir, like dtaxi/taxi-name_1241234.log
#    logfn = "{taxi_dir}/{taxi_name}_%j.log".format(**task_args)
#    taxi_call += " --open-mode append --output {logfn} ".format(logfn=logfn) # SLURM
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
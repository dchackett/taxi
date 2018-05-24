#!/bin/bash

#    Remarks on options:
#       
#       --partition Chooses the queue (a.k.a. "partition") to submit to (specified in local_queue)
#       --qos Chooses the quality of service (normal, debug, long, condo). Normal by default, so not provided here.
#       -A Chooses the allocation for the submission (specified in local_queue)
#       -N Specifies how many nodes to run on (specified in local_queue)
#       -t Specifies maximum walltime for job (specified in local_queue)
#       -J Name for job in the queue (specified in local_queue)
#       -o Path for output to be written to (specified in local_queue)

#SBATCH --exclusive
#SBATCH --qos=normal

# Load relevant modules
module load intel/16.0.3
module load impi/5.1.3.210
#module load gcc
#module load openmpi
module load python/2.7.11

# Load specified virtualenv, if provided
if [[ ! -z "${TAXI_PYENV// }" ]]; then
 source $TAXI_PYENV/bin/activate
fi

## Determine number of nodes, number of cpus
nNodes=$SLURM_JOB_NUM_NODES
(( nNodes= 0 + nNodes ))
coresPerNode=$SLURM_CPUS_ON_NODE
(( nCores = nNodes * coresPerNode ))


# "which" that doesn't care about executability
run_taxi_path=$(find ${PATH//:/ } -maxdepth 1 -name run_taxi.py -print -quit)

python $run_taxi_path $@
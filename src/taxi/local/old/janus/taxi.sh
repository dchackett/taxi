#!/bin/bash

#SBATCH --account UCB00000490
#SBATCH --qos janus

TaxiInstallDir='/projects/daha5747/taxi/'

# Put taxi install dir in PYTHONPATH if not there already
if ! echo $PYTHONPATH | grep -q $TaxiInstallDir; then
    export PYTHONPATH="${PYTHONPATH}:${TaxiInstallDir}"
fi

## Determine number of nodes, number of cpus
nNodes=$SLURM_JOB_NUM_NODES
(( nNodes= 0 + nNodes ))
coresPerNode=$SLURM_CPUS_ON_NODE
(( nCores = nNodes * coresPerNode ))

## Need to know where this script is to resubmit it
ThisScript="${TaxiInstallDir}/taxi.sh"

## Load MPIrun module
module load openmpi/openmpi-1.6.4_gcc-4.8.1_ib

export MPI_BINARY='mpirun'

python ${TaxiInstallDir}/taxi.py --nodes $nNodes --cpus $nCores --name $SLURM_JOB_NAME --shell $ThisScript $*

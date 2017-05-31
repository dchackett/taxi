#!/bin/bash

#SBATCH -p shas
#SBATCH --exclusive

TaxiInstallDir='/home/etne1079/python/taxi/'

# Put taxi install dir in PYTHONPATH if not there already
if ! echo $PYTHONPATH | grep -q $TaxiInstallDir; then
    export PYTHONPATH="${PYTHONPATH}:${TaxiInstallDir}"
fi

## Determine number of nodes, number of cpus
nNodes=$SLURM_JOB_NUM_NODES
(( nNodes= 0 + nNodes ))
coresPerNode=24
(( nCores = nNodes * coresPerNode ))

## Need to know where this script is to resubmit it
ThisScript="${TaxiInstallDir}/taxi.sh"

## Modules
module load intel/16.0.3
module load impi/5.1.3.210

## Settings for Intel MPI - hopefully these will become generic when Summit enters production
export MPI_BINARY='mpirun -genv I_MPI_FABRIC=shm:tmi -genv I_MPI_TMI_PROVIDER=psm2'

python ${TaxiInstallDir}/taxi.py --nodes $nNodes --cpus $nCores --name $SLURM_JOB_NAME --shell $ThisScript $*

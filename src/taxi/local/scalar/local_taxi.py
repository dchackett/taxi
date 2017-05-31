#!/usr/bin/env python

## Taxi info
taxi_install_dir = "/home/eneil/python/taxi"

taxi_shell_core = """
#!/bin/bash

TaxiInstallDir='{}'""".format(taxi_install_dir)

taxi_shell_core += """
# Put taxi install dir in PYTHONPATH if not there already
if ! echo $PYTHONPATH | grep -q $TaxiInstallDir; then
    export PYTHONPATH="${PYTHONPATH}:${TaxiInstallDir}"
fi

python ${TaxiInstallDir}/run_taxi.py $@
"""

## Binary locations
mpirun_str = "mpirun -np {0:d} "
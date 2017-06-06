__version__ = '0.2.0'

import os

## Utility functions
def flush_output():
    sys.stdout.flush()
    sys.stderr.flush()

def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)

from run_taxi import Taxi
# Disclaimer/Advertisement

taxi is presently under very active development. If you would like to use taxi for your work, please contact us if you need any assistance. We will gladly help you get up and running.

# Overview

taxi is a minimal scientific workflow manager intended to automate the process of running Markov Chain Monte Carlo (MCMC) simulations (and more specifically, lattice gauge theory (LGT) simulations) on large parallel clusters. For an overview of scientific workflow management, please consult the excellent [Pegasus documentation](https://pegasus.isi.edu/documentation/tutorial_scientific_workflows.php).

Most scientific workflow management systems (WMSs) are difficult to install on remote machines and may have unachievable (or highly nontrivial to work around) requirements (e.g., some WMS might only work with the HTCondor queueing system, but you need to run on a machine that uses Slurm or PBS). Furthermore, many of these systems are designed to accomodate much more general workflows than necessary for MCMC/LGT studies. This excess generality can make it difficult/time-prohibitive to learn how to use a WMS. Finally, most systems require some active central monitor program to be kept running; this program must retain access to remote resources, which can be difficult to manage.  taxi was made to address all of these issues: taxi is easy to get running on almost any machine with any queueing system, and designed with running MCMC/LGT simulations in mind.

taxi is intended to be lightweight, requiring a minimum of installation and setup to run on remote clusters. To this end, taxi is written to be compatible with Python 2.6.6, which is the version of Python available on most clusters (including the USQCD machines at Fermilab). The taxi package has minimal dependencies on non-standard Python packages, and works gracefully and transparently with virtualenv to allow installation on machines where users have limited privileges (taxi uses virtualenv roughly like a barebones container).

Most MCMC workflows share a common structure. taxi takes advantage of this structure to make it easy to adapt the software to run different binary suites, and to specify complicated workflows. taxi provides a number of abstract superclasses that run common types of MCMC binaries; adapting these to run a specific binary amounts to overriding two or three functions. A set of convenience functions allow users to specify arbitrarily long sequences of configuration-generations (e.g., HMC) and measurements on the resulting configurations (e.g., spectroscopy).

In order to circumvent the need for an active central monitor program, taxi uses a passive central control scheme. In this "taxi-dispatcher" model, a set of workers (taxis) are controlled by a central, passive controller (the dispatcher). The taxis iterate the following cycle:
1. Taxi completes its present task
2. Upon task completion, taxi calls up the dispatcher, informs it if the task completed successfully, and gives it diagnostic information about that task
3. Dispatcher considers the remaining tasks, tells the taxi what to work on next, and updates its records
4. Taxi hangs up and begins work on its new task.

The Dispatcher only needs to be active while a Taxi is communicating with it. This allows all of the processing portions of the Dispatcher to be run by whatever program implements the Taxis, and thus no central monitor program needs to be kept running and/or connected to the remote cluster. The Dispatcher's records/memory are stored in some way that is accessible to all worker jobs. The present implementation of taxi uses SQLite databases stored on shared file systems to implement the Dispatcher.

In order to run workflows that take longer than the maximum allowable run time for a job (e.g., 24 hours), taxis will resubmit themselves. The taxis will also adaptively manage the number of taxi worker jobs on the queue to match the available workload, without any input required from the user.

# How to use taxi

## Installing on USQCD/Fermilab machines

### Creating a virtual environment

Many computing resources (such as the USQCD cluster at Fermilab) do not have an up-to-date version of Python (most machines only have Python 2.6.6, versus Python 2.7).  The packages standardly included in scientific Python distributions are also rarely available.  Because users typically have limited privileges in such environments, it can be difficult to install or update anything.

The solution to this issue is virtualenv, which requires no installation and allows you to create an isolated Python environment. New virtualenvs have setuptools installed in them so that you may install and update whatever python packages you need.

To get a virtualenv set up (without any installation):
1. Download the `tar.gz` of the latest version of [virtualenv](https://pypi.python.org/pypi/virtualenv).
2. Copy the archive to the USQCD cluster, e.g.: `scp virtualenv-15.1.0.tar.gz some_user@bc1.FNAL.GOV:~`
3. Unpack it, e.g.: `tar xvzf virtualenv-15.1.0.tar.gz`.  Note that no installation is necessary.
4. Create a new virtual environment, e.g.: `virtualenv-15.1.0/virtualenv.py taxi_env`
5. Activate the virtual environment, e.g.: `source taxi_env/bin/activate`

We can now install taxi and the packages it depends on in this virtualenv. Whenever you want to use taxi (e.g., tp submit jobs or use tools), you must first activate the virtualenv like `source taxi_env/bin/activate`.


### Localizing and installing taxi

Ensure that the virtualenv created above is active for this part of the process (unless you have sufficient privileges to install modules in whatever Python you're working with, in which case a virtualenv is not necessary).

1. Make a clone of the taxi repository from GitHub, e.g.: `git clone https://github.com/dchackett/taxi.git` (or download and unpack an archive of the repository)
2. In the root folder of the repository, call `setup.py` to localize taxi: `python setup.py localize --machine=fnal`
3. Install taxi. In a virtualenv, you do not provide a `--user` flag: `python setup.py install`. Note that this step will automatically install `argparse` and `parse` from PyPI, which requires an internet connection.  If no internet connection is available, download and install these packages in the virtual environment beforehand.


## Overview of examples

We have provided several examples from our own work in `taxi/examples` to show the typical use cases of taxi. Each file therein is a run specification script. Running this script (with taxi appropriately localized and installed)  will specify a set of tasks to run and submit jobs to the batch queue (launch taxis) specified by the localization.

Coming soon: examples that run a toy binary suite, instead of requiring our MILC distribution.


### multirep_hmc

The most general case of MCMC workflow is generating sequences of configurations from scratch and performing measurements on these configurations. These configurations may comprise one or more ensembles. Some sequences may be executed in parallel.

In the folder `taxi/examples`, there are two files: `cu_multirep_hmc.py` and `fnal_multirep_hmc.py`. These are toy examples using the "multirep MILC" software suite used by the authors of this software in a recent research project. Both of these scripts specify and launch the same workflow, but for different machines: the local cluster used by the University of Colorado Lattice group, and the USQCD machines at Fermilab. (Note that these two files are different only where the user must specify the number of cores each taxi will use, and where the allocation must be specified for the USQCD machines. It is almost trivial to adapt a run specification script to run the same workflow on a different machine.)

The specified workflow will create four stored gauge configurations for a first ensemble of 4^4 lattices, and two configurations for a second ensemble.  Each configuration is separated by two trajectories.  The first ensemble begins from a fresh start (i.e., all links identity), while the second ensemble forks off from the second configuration in the sequence of configurations for the first ensemble (such that the third configuration of the first ensemble and first configuration of the second ensemble are generated simultaneously). Each configuration file is accompanied by an output file from the HMC binary that created the configuration. Each configuration (after the first in each ensemble, which is discarded for equilibration) will also have several measurements performed on it: spectroscopy for two different representations of Wilson fermion, and measurement of a variety of observables as the configuration is evolved under the Wilson flow.

The scripts `*_multirep_hmc.py` specify the workflow, then set up for and launch the jobs that will perform all the specified tasks. Initially, one worker job will be submitted to the queue. When this first taxi begins running, it generates the first two gauge configurations in the first ensemble. Upon finishing the second gauge configuration of the first ensemble, the second ensemble may now begin running; the first taxi will submit a second job to the queue (launch a second taxi). Assuming sufficiently short queue times, there will then be two taxis running on the workflow simultaneously.


### measure_mrep_spectro_on_files

In a common use case, you have some pool of stored gauge configurations generated long ago. You want to perform some new measurement on these, e.g., look at some new observable under Wilson flow, or run spectroscopy.

In the folder `taxi/examples`, there is a file `measure_mrep_spectro_on_files.py`.  This script reads a list of gauge file locations from the file `sample_data/gaugefiles`. It then specifies a workflow which will run spectroscopy for two different representations of fermion on each gauge file.  The script finally launches 20 taxis to work simultaneously on the workflow, which has a trivial dependency structure and so may have an arbitrary number of simultaneous workers (up to the number of tasks).


## Testing taxi

taxi comes with a suite of unit tests (still under development). The test suite takes advantage of (tox)[https://tox.readthedocs.io/] to automate testing as well as to test the software in a particular environment. To test taxi,
1. (Optional) activate the virtualenv you'd like to install the version to be tested in.
2. (Optional) delete the folders `taxi/bin`, `taxi/build`, `taxi/dist`, and `taxi/test_run` to ensure a clean install and testing environment (only called for in case of nonsensical errors, but does not hurt to do).
3. Run `python setup.py localize --machine=scalar` to plug in the scalar queue, a simplified local queue used for testing. 
4. Run `python setup.py install --user` to install the version of taxi to be tested.
5. Run `tox` to run the test suite.

For convenience, the included script `run_tests.sh` will install tox if it's not present and perform these actions.


## Command-line tools

taxi comes with a set of simple command-line tools to check on the progress of jobs, make minor adjustments to jobs in progress, and to recover from common failures.  setup.py will include these tools in the PATH (inside whatever virtualenv is active when installing taxi), so they are easily accessible from the command line without having to specify a long absolute path or make aliases. DISCLAIMER: the current toolset is largely ad-hoc and made by the authors as needed. Better tools are under development.

### taxi-summary

Usage: `taxi-summary {dispatch_db_filename}`

Prints out a digest of the state of the specified dispatch: number of tasks active, pending, completed, failed, abandoned, etc.  Prints out which taxis are running currently-active tasks.

### taxi-unabandon

Usage: `taxi-unabandon {dispatch_db_filename}`

Detects any abandoned tasks in the dispatch.  For each abandoned task, rolls the task back (i.e., deletes their outputs and sets their status to 'pending') and then relaunches the missing-in-action taxi that crashed and abandoned the task.

Useful in cases where taxis crash, or when taxi jobs are killed by hand (e.g. using qdel).

### taxi-spawn-idle-taxis

Usage: `taxi-spawn-idle-taxis {dispatch_db_filename} {pool_db_filename}`

Launches idle taxis for a given job.  This is only useful to restart work on a job where all taxis have crashed or been killed.  If any taxis survive, they will spawn all idle taxis between each task. Note both the dispatch and pool DBs must be specified.

### taxi-rollback

Usage: `taxi-rollback {dispatch_db_filename} {task_id}`

Roll back the specified task, i.e., move all of its output files to `./rollback/` (instead of deleting them) and set the job for pending; repeat recursively for any completed or failed jobs that depend on the specified job.

### taxi-edit-task

Usage: `taxi-edit-task {dispatch_db_filename} {task_id} {param_name} {new_value}`

Sets the parameter named `param_name` to the specified value in specified task.  Attemps to guess the datatype of `new_value` by looking at the previous value (if set), or trying to cast the value to an integer or float before leaving it as a string.

### taxi-edit-stream

Usage: `taxi-edit-stream {dispatch_db_filename} {task_id} {param_name} {new_value}`

Like `taxi-edit-task`, but applies the change to not only to the specified task, but to all tasks of the same type downstream. Used for editing parameters in a sequence of ConfigGenerators, e.g., integrator step size like `taxi-edit-stream disp.sqlite 1 nsteps1 12`. Will not cascade through a Task marked as a `branch_root`, so changes will not be applied to a stream forked off to generate a new ensemble.

### taxi-edit

Usage: `taxi-edit {dispatch_db_filename} [{pool_db_filename}]`

Hacky power tool. Loads an interactive Python environment with the following in scope:
- `d`, an instance of SQLiteDispatcher connected to the provided dispatch DB.
- `p`, an instance of SQLitePool connected to the provided pool DB (if provided).
- `q`, an instance of the LocalQueue for whatever localization taxi was installed with.
- `tb`, a dictionary like { (task id) : (reconstructed Task instance) }.  Note that changing the tasks in this dictionary will not change the tasks in the dispatch DB.  In order to do so, call `d.write_tasks(tb.values())`, or to be more cautious `d.write_tasks([list of affected tasks])`.
- `tbf`, a list of reconstructed Task objects for all failed tasks in the dispatch. (Same warning as for `tb`).
- `tba`, a list of reconstructed Task objects for all activate tasks in the dispatch. (Same warning as for `tb`).
- `tbcg`, a list of reconstructed Task objects for all ConfigGenerators in the dispatch. (Same warning as for `tb`).
- `cgtrees`, list of lists, with each sublist containing separate ConfigGenerator streams (e.g., each sublist contains all the ConfigGenerator tasks for a given ensemble).
- `trees`, list of lists, with each sublist containing a ConfigGenerator stream and all of its dependent tasks (e.g., each sublist contains all ConfigGenerator tasks for a given ensemble, measurements to be performed on the resulting saved configurations, Copy jobs to file outputs, etc.)
- `tl`, a list of reconstructed Taxi objects (if a path to the pool DB is provided). As with the Tasks in `tb*`, these objects are not synchronized with the pool DB file, which can be updated by calls to e.g., `p.update_taxi_status(...)`.

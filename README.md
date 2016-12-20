
How to use:

Clone the repo wherever you want.  In the repo folder, run "python INSTALL.py --machine [xxx]" where [xxx] in ['cu', 'fnal', 'janus'].  This is most of what is required to get the system working.  Beyond this, you only need to specify locations in the filesystem in each job-specification script: where the runner scripts and binaries are located, where to store outputs and gauge configs, etc.  There are convenience functions to do this parsimoniously.

There are examples of how to specify and launch streams of HMC jobs and auxiliary (spectro, flow) jobs in 'taxi/examples/'.  I've commented them very heavily, explaining what everything does.  I hope it should be enough to get off the ground.  I'd recommend running one of them to see exactly what it does, how it places files, etc.  Each example only runs 20 4^4 hmc jobs (+ auxiliaries), so they don't take long/cost any time.

The comments are not quite as fleshed out in janus_example.py, but all three examples are almost exactly the same in actual content.



Design paradigms and features:

Taxi-Dispatcher model --
Any workflow management system (including auto-pbs, in abstract) has a bunch of worker tasks working on a large semi-shared pool of tasks.  How to distribute the tasks among the workers is a problem with many solutions.

Auto-pbs has each worker hardcoded to chew threw a sequence of HMC tasks.  In a sense, this is the minimally-flexible way of assigning tasks ("brick-on-gas-pedal model"?).

In my old CU-cluster job manager, I use a different model ("overlord-minion"?) for assigning tasks to workers.  The workers in this model are the nodes on the beowulf cluster (for conceptual clarity, assume Will does not have thousands of spectroscopy jobs in the queue, so submitted jobs start running immediately).  The "overlord" sends "minions" off to perform discrete tasks ("run the HMC binary"; "run the spectroscopy binary"; etc.).  After completion, the minions are free to accept a new task (nothing is running on the node).  The overlord actively monitors the queue, keeping track of dependencies.  When it sees that a minion has finished a task (a job falls out of the queue), it updates the dependency structure.  The minion is unoccupied (node has nothing running on it), so the overlord assigns it a new task (submits a new job).

The overlord-minion model has advantages (maximizing resource usage).  However, the overlord must actively monitor the queue, which means a program must be running to make progress through the task pool.  Progress can be stopped for silly reasons (laptop battery died) and it's very inconvenient to use an active queue monitor on a remote cluster.

The "taxi-dispatcher" model (I made this terminology up, realistically somebody's probably already thought of this and named it something else) is a similar approach (central allocation of tasks to workers), but with a passive overlord.  The workers ("taxis") receive orders ("run this script with these parameters") from a central source ("dispatcher").  They then run off and complete their assigned tasks.  After their assignment is complete, they ask the dispatcher for new orders.  The distinction between overlord-minion and taxi-dispatcher is exactly: overlords give orders to idle minions; idle taxis ask for orders from dispatchers.  This inversion allows for the dispatcher to be a passive entity, i.e., no program needs to be running at all times to monitor the workers.

In this software, the role of the dispatcher is played by an sqlite database.  The database (not perfectly) makes sure that requests for orders are handled serially, preventing multiple workers from working on the same task.


Self-submitting Worker --
Clusters like janus and those at Fermilab have time limits for how long a job may take.  This is usually 24 hours, which usually isn't long enough to run an entire ensemble.  So, multiple jobs must be spawned in sequence to work on the same task pool.

In the overlord-minion model, the overlord is a program that monitors the queue.  When it sees a worker has died, it can simply launch a new worker.  However, in the taxi-dispatcher model, the dispatcher is passive and cannot monitor the queue.  So, the taxis must be able to persist longer than the time limit.  This is accomplished via self-resubmission after the worker's allotted time has been used up.  This behavior is familiar from auto-pbs.


Forests of tasks --
The pool of tasks associated with generating and analyzing a bunch of ensembles is naturally structured like a forest (a group of trees).  Each task depends on other tasks (e.g., a spectroscopy binary needs a gauge configuration to run on, so a spectroscopy-running task depends on the HMC-running task that creates the gauge configuration).  Drawing tasks as nodes and dependencies as (directed) edges, we arrive at a (hopefully) acyclic graph: a tree.  If some streams of tasks happen to be in the same task pool but are completely unconnected via dependencies, then they comprise separate trees.  The task pool thus contains multiple task trees, and is a task forest.

The tasks in a task forest can be further categorized as "trunk" and "non-trunk" tasks.  Trunks are simply the tasks that dominate the runtime.  For our purposes, HMC tasks are "trunks" and all other tasks are "non-trunks".  Optimally, we will want one worker working on every trunk in the job forest that is accessible (because all of its dependencies have been resolved).


Self-adjusting nsteps --
I have spent lots of time doing the following on Janus: run a few configurations, look at the accept rate, change nsteps up or down by hand, repeat.  This is very robotic, which is a good indication that robots should be doing it instead.  There's a species of task called "NstepAdjustor" which will look at runs preceding an HMC job, check their acceptance rate, and tune nsteps for the upcoming HMC run up and down to keep the AR in a specified range.  The line that calls the convenience function that implements this is present in both "cu_example_adaptive.py" and "fnal_example.py"

#!/usr/bin/env python
import time
import os
import sys

def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)

class CopyTaskException(Exception):
    pass

class RunScriptTaskException(Exception):
    pass

class Taxi(object):

    def __init__(self, name=None, pool_name=None, time_limit=None, cores=None):
        self.name = name
        self.pool_name = pool_name
        self.time_limit = time_limit
        self.cores = cores
        self.time_last_submitted = None
        self.start_time = None  ## Not currently saved to DB, but maybe it should be?
        self.status = 'I'

    def __eq__(self, other):
        eq = (self.name == other.name)
        eq = eq and (self.pool_name == other.pool_name)
        eq = eq and (self.time_limit == other.time_limit)
        eq = eq and (self.cores == other.cores)
        eq = eq and (self.time_last_submitted == other.time_last_submitted)
        eq = eq and (self.start_time == other.start_time)
        eq = eq and (self.status == other.status)

        return eq

    def taxi_name(self):
        return '{0:s}_{1:d}'.format(self.pool_name, self.name)

    def rebuild_from_dict(self, taxi_dict):
        try:
            self.name = taxi_dict['name']
            self.pool_name = taxi_dict['pool_name']
            self.time_limit = taxi_dict['time_limit']
            self.cores = taxi_dict['cores']
            self.time_last_submitted = taxi_dict['time_last_submitted']
            self.status = taxi_dict['status']
        except KeyError:
            print "Error: attempted to rebuild taxi from malformed dictionary:"
            print taxi_dict
            raise
        except TypeError:
            print "Error: type mismatch in rebuilding taxi from dict:"
            print taxi_dict
            raise

    def to_dict(self):
        return {
            'name': self.name,
            'pool_name': self.pool_name,
            'time_limit': self.time_limit,
            'cores': self.cores,
            'time_last_submitted': self.time_last_submitted,
            'start_time': self.start_time,
            'status': self.status,
        }

    def __repr__(self):
        return "Taxi<{},{},{},{},{},'{}'>".format(self.name, self.pool_name, self.time_limit,
            self.cores, self.time_last_submitted, self.status)


    def execute_task(self, task):
        """Execute the given task, according to task['task_type']."""
        
        # Record start time
        self.task_start_time = time.time()

        # Run task
        task_type = task['task_type']
        if (task_type == 'die'):
            print "DIE: killing taxi {}".format(self)
            sys.exit(0)
        elif (task_type == 'copy'):
            # Copy
            task_args = task['task_args']
            if not os.path.exists(task_args['src']):
                fail_str = "Copy failed: file" + task_args['src'] + "does not exist, cannot copy it"
                raise CopyTaskException(fail_str)
            else:
                # Dig out destination directory if it's not there already
                if not os.path.exists(os.path.split(task_args['dest'])[0]):
                    mkdir_p(os.path.split(task_args['dest'])[0])            
                # Copy!
                print "Copying", task_args['src'], "->", task_args['dest']
                exit_code = os.system('rsync -Paz {src} {dest}'.format(**task_args))
                if (exit_code >> 8) != 0: # only care about second byte
                    raise CopyTaskException("Copy failed: system call returned {}".format(exit_code))

        elif (task_type == 'run_script'):
            # Run script
            print "Running script", task_args['script']
            flush_output()
            
            cmd_line_args = []
            if task_args.has_key('cmd_line_args'):
                # Args as list are legacy, cut out when all old dispatches are complete
                if isinstance(task_args['cmd_line_args'], list):
                    cmd_line_args += task_args['cmd_line_args']
                elif isinstance(task_args['cmd_line_args'], dict):
                    cmd_line_args += ["--{k} {v}".format(k=k, v=v) for (k,v) in task_args['cmd_line_args'].items()]
            
            # Specify number of cpus per the provided formatter
            if task_args.has_key('ncpu_fmt'):
                cmd_line_args.append(task_args['ncpu_fmt'].format(cpus=parg.cpus))
                
            # Explicitly call python to run scripts
            shell_call = " ".join(['python', task_args['script']] + map(str,cmd_line_args))
            
            # Run the script, catch exit code
            exit_code = os.system(shell_call)
            
            if (exit_code >> 8) != 0: # only care about second byte
                raise RunScriptTaskException("run_script: Script exit code = {exit_code} != 0".format(exit_code=exit_code))
            
        else:
            raise ValueError("Invalid task type specified: {}".format(task_type))


    
#!/usr/bin/env python

# Definition of "Dispatcher" class - manages task forest and assigns tasks to Taxis.

import os
import json
from taxi._utility import LocalEncoder

import imp # For dynamical imports
import __main__ # To get filename of calling script

import taxi
import taxi.tasks as tasks

## Need to be able to make blank objects to reconstruct Tasks from JSON payloads
class BlankObject(object):
    def __init__(self):
        pass # Need an __init__ function to have a __dict__
        
        
def task_priority_sort_key(task):
    """For use as argument in sorted(..., key=task_priority_sort_key).
    
    Order tasks by their priority score.
    Negative priority (default -1) is the lowest.
    For positive priority, smaller numbers are higher priority.
    
    Enforces ordering by returning (False, p) for any positive priority p, and (True, p)
    for any negative priority p.
    """
    return (task.priority < 0, task.priority)


class TaskClaimException(Exception):
    pass



class Dispatcher(object):

    def __init__(self):
        pass

    def __enter__(self):
        raise NotImplementedError
        
    def __exit__(self):
        raise NotImplementedError

    def _create_new_dispatch(self):
        pass
        
    
    def _load_existing_dispatch(self):
        # Just need to get these in to the global namespace somewhere so the
        # task subclasses can be found
        self._imported = [imp.load_source('mod%d'%ii, I) for ii, I in enumerate(self.imports)]
        
        # Print valid task classes that have been loaded
        print "Loaded Task subclasses:", taxi.all_subclasses_of(taxi.tasks.Task)
        

    def get_task_blob(self, my_taxi=None, include_complete=True):
        """Get all incomplete tasks pertinent to this taxi."""
        raise NotImplementedError


    def check_task_status(self, task):
        """Quick query of task status for task with id=task_id from task forest DB.

        For last-minute checks that task hasn't been claimed by another task."""

        raise NotImplementedError


    ## Taxi interface
    def request_next_task(self, for_taxi):
        """Determines the next task to be executed by the given taxi."""
        
        task_blob = self.get_task_blob(for_taxi, include_complete=False)

        # Order tasks in blob by priority
        if (task_blob is None) or (len(task_blob) == 0):
            task_priority_ids = []
        else:
            task_priority_ids = [ t.id for t in sorted(task_blob.values(), key=task_priority_sort_key) ]
        
        # Find highest-priority task that can be completed
        N_pending_tasks = 0
        N_blocked_by_time = 0 # Tasks 'blocked by time' are ready to go, but not enough time to run
        found_ready_task = False
        for task_id in task_priority_ids:
            task = task_blob[task_id]

            # Only try to do pending tasks
            if task.status != 'pending':
                continue
            
            N_pending_tasks += 1
                
            # Check whether task is ready to go, and taxi can run it
            N_unresolved, N_failed = task.count_unresolved_dependencies()
            sufficient_time = for_taxi.enough_time_for_task(task)
            
            if not sufficient_time and N_unresolved == 0:
                N_blocked_by_time += 1
            
            # Look deeper in priority list if task not ready
            if N_unresolved > 0 or not sufficient_time:
                continue
            
            # Task ready; stop looking for new task to run
            found_ready_task = True
            break


        # If there are no tasks, finish up
        if N_pending_tasks == 0:
            ## TODO: I think this will break both the with: and the outer while True:,
            ## but add a test case!
            return tasks.Die(message="WORK COMPLETE: no tasks pending")
            
        if not found_ready_task:
            ## TODO: we could add another status code that puts the taxi to sleep,
            ## but allows it to restart after some amount of time...
            ## Either that, or another script somewhere that checks the Pool
            ## and un-holds taxis that were waiting for dependencies to resolved
            ## once it sees that it's happened.
            ## Also need to be wary of interaction with insufficient time check,
            ## which we should maybe track separately.
            if N_blocked_by_time > 0:
                # Just need more time -- tell this taxi to resubmit itself!
                return tasks.Respawn()
            else:
                # Something is wrong other than not having enough time.
                return tasks.Sleep(message="WORK COMPLETE: no tasks ready, but %d pending"%N_pending_tasks)
        
        # If we've gotten this far, successfully found a pending task.
        return task
        

    def write_tasks(self, tasks):
        raise NotImplementedError


    def claim_task(self, my_taxi, task):
        raise NotImplementedError


    def finalize_task_run(self, my_taxi, task):
        if task.status != 'failed':
            if task.is_recurring:
                task.status = 'pending'
            else:
                task.status = 'complete'

        # Write local (completed) version of the task to the dispatch DB
        # ...unless we don't have an id for it, in which case it's not in the DB or we can't find it
        if getattr(task, 'id', None) is not None:
            self.write_tasks([task])
            
            
    def mark_abandoned_task(self, by_taxi):
        """Method for when a Taxi has died unexpectedly.  When this occurs, it often means
        a task is left marked 'active', but has in fact failed(/been abandoned).  This method marks that task
        abandoned.
        """
        
        by_taxi = str(by_taxi)
        assert by_taxi is not None # This should never happen, but would be catastrophically inconvenient if it did.
        
        tasks = self.get_task_blob()
        abandoned_tasks = []
        for task_id, task in tasks.items():
            if task.status == 'active' and task.by_taxi == by_taxi:
                task.status = 'abandoned'
                print "WARNING: Task {tid} was abandoned by taxi {tn}.".format(tid=task_id, tn=by_taxi)
                abandoned_tasks.append(task)
        
        # NOTE: At present, a taxi shouldn't be able to run multiple tasks at once
        # ...but this way, if we allow them to do so, this routine will still work.
        self.write_tasks(abandoned_tasks)
        
        
    def _trunk_number(self, task_blob, for_taxi=None):
        """Determines the number of trunks that are available to work on (roughly,
        the number of taxis that should be actively working on a task forest).
        
        Returns an int, which is the count of running or pending-but-ready trunk tasks.
        """
        if task_blob is None or len(task_blob) == 0:
            return 0
        
        task_blob = [t for t in task_blob.values() if t.trunk] # filter out non-trunks, task_blob is now list(task)
        
        # If we're asking about a particular taxi, filter for tasks that taxi can run
        if for_taxi is not None:
            for_taxi = str(taxi)
            task_blob = [t for t in task_blob if t.for_taxi==for_taxi]
        
        N_active_trunks = 0
        for task in task_blob:
            if task.status == 'active':
                N_active_trunks += 1
                continue
            
            if task.status != 'pending':
                continue
                
            # Check whether task is ready to go, and taxi can run it
            N_unresolved, N_failed = task.count_unresolved_dependencies()
            if N_unresolved == 0:
                N_active_trunks += 1

        return N_active_trunks    
    
    
    def _N_ready_tasks(self, task_blob, for_taxi=None):
        """Looks through task_blob and counts how many tasks are ready that can
        only be run by the taxi specified in for_taxi.
        """
        if task_blob is None or len(task_blob) == 0:
            return 0
        
        # Filter for tasks that are pending and only for the taxi specified
        task_blob = [t for t in task_blob.values() if t.status == 'pending']
        
        if for_taxi is not None:
            for_taxi = str(for_taxi)
            task_blob = [t for t in task_blob if t.for_taxi == for_taxi] # Specifically tasks for this taxi
        
        N_ready_tasks = 0
        for task in task_blob:
            # Check whether task is ready to go, and taxi can run it
            N_unresolved, N_failed = task.count_unresolved_dependencies()
            if N_unresolved == 0:
                N_ready_tasks += 1
        return N_ready_tasks
    
    
    def should_taxis_be_running(self, taxi_list):
        """Determines whether tasks are available for each taxi to run.
        
        Taxis should be run if there are trunks available for them
        
        Args:
            taxi_list: List of taxi objects; are there tasks available for these taxis to run?
        Returns:
            Dictionary like {(taxi object) : (should taxi be running?)}
        """
        
        task_blob = self.get_task_blob(None, include_complete=False) # dict(id:task)
    
        # There's nothing we can do with errored E or held H taxis
        taxi_list = [t for t in taxi_list if t.status in ['Q', 'R', 'I']] # Only want queued, running, or idle taxis
        
        # We only care taxis running on this dispatch
        taxi_list = [t for t in taxi_list if taxi.expand_path(t.dispatch_path) == taxi.expand_path(self.db_path)]
        
        # Convenient dictionary like {(name of taxi) : (taxi object)}
        taxi_dict = {}
        for my_taxi in taxi_list:
            taxi_dict[str(my_taxi)] = my_taxi
    
        # Initial desired state is the present state -- idle taxis idle, active taxis active
        # Desired state is a dict like { str(taxi_name) : (should taxi be active?) }
        desired_state = {}
        for my_taxi in taxi_list:
            desired_state[str(my_taxi)] = my_taxi.status in ['Q', 'R'] # Active means queued or running
        
        # If taxi has a trunk only it can run, or some tasks are ready that only this taxi can run, it must be running
        for my_taxi in taxi_list:
            if self._trunk_number(task_blob, for_taxi=my_taxi) > 0:
                desired_state[str(my_taxi)] = True
            if self._N_ready_tasks(task_blob, for_taxi=my_taxi) > 0:
                desired_state[str(my_taxi)] = True
                
        # With taxi-specific requirements imposed, now just make sure we have enough taxis running
        active_taxis = [taxi_dict[k] for (k,v) in desired_state.items() if v]
        idle_taxis = [taxi_dict[k] for (k,v) in desired_state.items() if not v]
        
        N_active_taxis = len(active_taxis)
        
        N_active_trunks = self._trunk_number(task_blob)
        
        # Even without trunks, if we have tasks that are ready, we need at least one taxi
        N_ready_tasks = self._N_ready_tasks(task_blob)
        if N_active_trunks == 0 and N_ready_tasks:
            N_active_trunks = N_ready_tasks # Correct behavior for trunkless task forests
        
        # Activate idle taxis until we have enough
        for my_taxi in idle_taxis:
            if N_active_taxis >= N_active_trunks:
                break # We have enough taxis running
            desired_state[str(my_taxi)] = True
            N_active_taxis += 1
            
        return desired_state
        
        
    def _invert_dependency_graph(self, task_pool):
        # Give each task an identifier, reset dependents
        for jj, task in enumerate(task_pool):
            task._dependents = []

        # Let dependencies know they have a dependent
        for task in task_pool:
            if task.depends_on is None:
                continue
            for dependency in task.depends_on:
                dependency._dependents.append(task)
                

    ## Initialization
    def find_branches(self, task_pool):
        ## Scaffolding
        self._invert_dependency_graph(task_pool)
                
        ## Break apart tasks into separate trees
        # First, find all roots
        trees = []
        for task in task_pool:
            if task.depends_on is None or len(task.depends_on) == 0:
                trees.append([task])

        ## Build out from roots
        # TODO:
        # - If dependent has different number of nodes, make it a new tree
        # - If task is a trunk task and two dependents are trunk tasks, make one of them a new tree
        for tree in trees:
            for tree_task in tree:
                if not tree_task.trunk:
                    continue
                n_trunks_found = 0
                for d in tree_task._dependents:
                    # Count number of trunk tasks encountered in dependents, fork if this isn't the first
                    if d.trunk:
                        n_trunks_found += 1
                        if n_trunks_found > 1:
                            trees.append([d]) # Break branch off in to a new tree
                            continue
                    # Normal behavior: build on current tree
                    tree.append(d)
        
        return trees
                
    def _find_lowest_task_priority(self, task_pool):
        lowest_priority = 0
        for task in task_pool:
            if task.priority > lowest_priority:
                lowest_priority = task.priority

        return lowest_priority


    def _assign_priorities(self, task_pool, priority_method):
        """
        Assign task priorities to the newly tree-structured task pool.  Respects
        any user-assigned priority values that already exist.  All auto-assigned
        tasks have lower priority than user-chosen ones.

        'priority_method' describes the algorithm to be used for assigning priority.
        Currently, the following options are available:

        - 'tree': Tree-first priority: the workflow will attempt to finish an entire tree
        of tasks, before moving on to the next one.
        - 'trunk': Trunk-first priority: the workflow will attempt to finish all available
        tasks at the same tree depth, before moving deeper.
        - 'canvas': Or "anti-trunk".  Workflow will work on trunk tasks last, working through
        the tree layer-by-layer.
        - 'anarchy': No priorities are automatically assigned.  In the absence of user-determined
        priorities, the tasks will be run in arbitrary order, except that dependencies will be
        resolved first.
        """
        lowest_priority = self._find_lowest_task_priority(task_pool)

        if priority_method == 'tree':
            for tree in self.trees:
                tree_priority = lowest_priority + 1
                lowest_priority = tree_priority

                for tree_task in tree:
                    if (tree_task.priority < 0):
                        tree_task.priority = tree_priority
            
            return

        elif priority_method == 'trunk':
            for task in task_pool:
                if task.priority < 0:
                    if task.trunk:
                        task.priority = lowest_priority + 1
                    else:
                        task.priority = lowest_priority + 2
            return

        elif priority_method == 'canvas':
            for task in task_pool:
                if task.priority < 0:
                    if task.trunk:
                        task.priority = lowest_priority + 2
                    else:
                        task.priority = lowest_priority + 1
            return
            
        elif priority_method == 'anarchy':
            ## Do nothing
            return

        else:
            raise ValueError("Invalid choice of priority assignment method: {0}".format(priority_method))


    def _assign_task_ids(self, task_pool):
        # If we are adding a new pool to an existing dispatcher, 
        # start enumerating task IDs at the end
        start_id = self._get_max_task_id()
        
        # Give each task an integer id
        for jj, task in enumerate(task_pool):
            task.id = jj + start_id + 1
            

    def _populate_task_table(self, task_pool):
        self.write_tasks(task_pool)


    def _get_max_task_id(self):
        raise NotImplementedError


    def _store_imports(self):
        raise NotImplementedError


    def initialize_new_task_pool(self, task_pool, priority_method='canvas', imports=None):
        ## imports: Dispatcher needs to be able to import relevant runners.
        ## Convenient default behavior: import the calling script (presumably, the run-spec script)
        if imports is None:
            self.imports = [taxi.expand_path(__main__.__file__)] # Import the file that called this pool (presumably, run-spec script)
        else:
            self.imports = imports
        ## Store imports in the dispatch metadata
        self._store_imports()
            
        ## Build dispatch
        self.trees = self.find_branches(task_pool)
        self._assign_priorities(task_pool, priority_method=priority_method)
        self._assign_task_ids(task_pool)
        self._populate_task_table(task_pool)


    ## Cascading rollback
    def rollback(self, tasks, delete_files=False, rollback_dir=None):
        """Rolls back a task (or tasks) and any tasks that depend on it, and any tasks that depend on those, etc.
        """
        
        if not hasattr(tasks, '__iter__'): # Passed a single task, presumably
            tasks = [tasks]
        
        assert not (rollback_dir is None and delete_files == False),\
            "Must either provide a rollback_dir to copy files to or give permission to delete_files"
        
        task_blob = self.get_task_blob(include_complete=True)
        
        affected_tasks = []
        
        for task in tasks:
            # Find the task to roll back in the new task blob (objects won't be identical, but ids will)
            # TODO: Use task equality instead of ids?
            assert task_blob.has_key(task.id), "Can't find task to roll back in rollbackable tasks"
            task = task_blob[task.id]
            
            # Find rollbackable (non-active, non-pending) tasks (also dict->list)
            task_blob = [t for t in task_blob.values() if t.status not in ['active', 'pending']]
            
            # Find dependents
            self._invert_dependency_graph(task_blob)
            
            # Could do this with recursion instead, but recursion is slow in Python
            cascade_tasks = [task]
            tasks_to_roll_back = []
            while len(cascade_tasks) > 0:
                # Pop task to roll back off front of list
                rt = cascade_tasks[0]
                cascade_tasks = cascade_tasks[1:]
                
                if rt.status == 'active':
                    print "Can't rollback active task w/ id={0}. Kill it first.".format(task.id)
                    # Cancel rollbacking
                    tasks_to_roll_back = [] 
                    break
                
                tasks_to_roll_back.append(rt)
                
                # Must roll back everything downstream, add to list
                for d in rt._dependents:
                    if d not in task_blob:
                        continue # Not rollbackable
                    cascade_tasks.append(d)
                    
            for rt in tasks_to_roll_back:
                # Perform rollback
                affected_tasks.append(rt)
                rt._rollback(delete_files=delete_files, rollback_dir=rollback_dir)
            
        # Update DB
        self.write_tasks(affected_tasks)
            
    

    
    
import sqlite3

class SQLiteDispatcher(Dispatcher):
    """
    Implementation of the Dispatcher abstract class using SQLite as a backend.
    """ 

    ## NOTE: There is a little bit of code duplication between this and SQLitePool.
    ## However, the SQL is too deeply embedded in the pool/dispatched logic
    ## for a separate "DB Backend" object to make much sense to me.
    ##
    ## The clean way to remove the duplication would be multiple inheritance of
    ## an SQLite interface class...but I think multiple inheritance is kind of
    ## weird in Python2 and earlier.  We can look into it.


    def __init__(self, db_path):
        self.db_path = taxi.expand_path(db_path)
        self._setup_complete = False
        
        self._in_context = False
        
        with self:
            pass # Semi-kludgey creation/retrieval of dispatch DB
    

    def _create_new_dispatch(self):
        self.write_table_structure()
        
        super(SQLiteDispatcher, self)._create_new_dispatch()
        
        
    def _load_existing_dispatch(self):
        ## Get imports
        imports_query = """SELECT * FROM imports"""
        self.imports = [ii['import'] for ii in self.execute_select(imports_query)] # Extract list of imports from rows (dicts)
        
        ## Call super to do dynamical imports
        super(SQLiteDispatcher, self)._load_existing_dispatch()


    ## NOTE: enter/exit means we can use "with <SQLiteDispatcher>:" syntax
    def __enter__(self):
        # Don't allow layered entry
        if self._in_context:
            return
        self._in_context = True
        
        dispatch_db_exists = os.path.exists(self.db_path)
            
        self.conn = sqlite3.connect(self.db_path, timeout=30.0) # Creates file if it doesn't exist
        self.conn.row_factory = sqlite3.Row # Row factory for return-as-dict

        # Only run initializers once
        if not self._setup_complete:
            self._setup_complete = True
            if dispatch_db_exists:
                self._load_existing_dispatch()
            else:
                self._create_new_dispatch()
            
        ## Get/update a dictionary of all Task subclasses in the global scope, to
        ## rebuild objects from JSON payloads
        self.class_dict = taxi.all_subclasses_of(taxi.tasks.Task)


    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.conn.close()
        self._in_context = False


    def write_table_structure(self):
        create_task_str = """
            CREATE TABLE IF NOT EXISTS tasks (
                id integer PRIMARY KEY,
                task_type text,
                depends_on text,
                status text,
                for_taxi text,
                by_taxi text,
                is_recurring bool,
                
                req_time integer DEFAULT 0,
                start_time real DEFAULT -1,
                run_time real DEFAULT -1,
                priority integer DEFAULT -1,
                
                payload text
            )"""
            
        create_imports_str = """
            CREATE TABLE IF NOT EXISTS imports (
                id integer PRIMARY KEY,
                import text
            )"""

        with self.conn:
            self.conn.execute(create_task_str)
            self.conn.execute(create_imports_str)

        return


    def execute_select(self, query, *query_args):
        # If we're not in context when this is called, get in context
        if not self._in_context:
            with self:
                res = self.execute_select(query, *query_args)
            return res
        
        try:
            with self.conn:
                res = self.conn.execute(query, query_args).fetchall()
        except:
            raise

        return res


    def execute_update(self, query, *query_args):
        # If we're not in context when this is called, get in context
        if not self._in_context:
            with self:
                self.execute_update(query, *query_args)
            return
        
        # Semi-intelligent behavior for whether to execute or executemany
        # If query_args is nothing but tuples (from unpacking [(1,...), (2,...), ...]), then many
        # Otherwise, execute one
        use_execute_many = all([isinstance(qa, tuple) for qa in query_args])
        
        try:
            with self.conn:
                if not use_execute_many:
                    self.conn.execute(query, query_args)
                else:
                    self.conn.executemany(query, query_args)
                self.conn.commit()
        except:
            print "Failed to execute query: "
            print query
            print "with arguments: "
            print query_args
            raise


    def register_taxi(self, my_taxi, my_pool):
        my_taxi.dispatch_path = self.db_path
        my_pool.update_taxi_dispatch(my_taxi, self.db_path)


    def rebuild_json_task(self, r):
        # SQLite doesn't support arrays -- Parse dependency JSON in to list of integers
        if r.get('depends_on', None) is not None:
            r['depends_on'] = json.loads(r['depends_on'])
        
        # Big complicated dictionary of task args in JSON format
        if r.has_key('payload'):
            r['payload'] = json.loads(r['payload'])
                
        if self.class_dict.has_key(r['task_type']):
            task_class = self.class_dict[r['task_type']]
        else:
            raise TypeError("Unknown task_type '%s'; Task subclass probably not imported."%r['task_type'])
            
        rebuilt = BlankObject()
        #rebuilt.__dict__ = r # Python objects are dicts with dressing, pop task dict in to Task object
        rebuilt.__class__ = task_class # Tell the reconstructed object what class it is
        #rebuilt.__dict__.update(rebuilt.__dict__.pop('payload', {})) # Deploy payload
        
        # Set attributes appropriately -- set task class first for proper dynamic behavior
        for k, v in r.items():
            try:
                setattr(rebuilt, k, v)
            except AttributeError:
                pass # For non-settable properties
                
        # Deploy payload
        for k, v in rebuilt.__dict__.pop('payload', {}).items():
            try:
                setattr(rebuilt, k, v)
            except AttributeError:
                pass # For non-settable properties
        
        return rebuilt
            

    def get_task_blob(self, my_taxi=None, include_complete=True):
        """Get all incomplete tasks runnable by specified taxi (my_taxi), or all
        tasks (if my_taxi is not provided)."""

        if (my_taxi is None):
            task_query = """
                SELECT * FROM tasks """
            if (not include_complete):
                task_query += """ WHERE (status != 'complete')"""

            task_res = self.execute_select(task_query)
        else:
            taxi_name = str(my_taxi)

            task_query = """
                SELECT * FROM tasks
                WHERE (for_taxi=? OR for_taxi IS null)"""
            if (not include_complete):
                task_query += """ AND (status != 'complete')"""
        
            task_res = self.execute_select(task_query, taxi_name)
        

        if len(task_res) == 0:
            return None

        # Dictionaryize everything
        task_res = map(dict, task_res)
        
        # Objectify and package as task_id : task dict
        res_dict = {}
        for r in task_res:
            res_dict[r['id']] = self.rebuild_json_task(r)    
        
        
        # Replace ID dependencies with object dependencies
        for task_id, task in res_dict.items():
            if task.depends_on is None:
                continue
            
            # If not found in dictionary, just leave as IDs (usually don't request completes)
            task.depends_on = [(res_dict[dep_id] if res_dict.has_key(dep_id) else dep_id) for dep_id in task.depends_on]
        
        return res_dict


    def check_task_status(self, task):
        """Quick query of task status for task with id=task_id from task forest DB.

        For last-minute checks that task hasn't been claimed by another task."""
        
        if not hasattr(task, 'id'):
            ## Case: Dispatcher returns 'Die' to a taxi when it wants it to stop running.
            ## This 'Die' does not have an id, but we want it to run.
            return 'pending'

        task_res = self.execute_select("""SELECT status FROM tasks WHERE id=?""", task.id)
        
        if len(task_res) == 0:
            return None
        
        return dict(task_res[0])['status']


    def claim_task(self, my_taxi, task):
        """Attempt to claim task for a given taxi.  Fails if the status has been changed from pending."""

        # If task has no id, either it's not in the DB or we couldn't claim it anyways
        if getattr(task, 'id', None) is not None:
            # Claim in DB -- check for race conditions
            with self.conn:
                # Try to change status, but with condition that taxi status is pending
                claim_query = """UPDATE tasks SET status="active", by_taxi=? WHERE id=? AND status="pending";"""
                # Affected rows == 1 if this worked correctly
                result = self.conn.execute(claim_query, (my_taxi.name, task.id))
                claim_failed = result.rowcount != 1
                
                if claim_failed:
                    task_status = self.check_task_status(task)
                    raise TaskClaimException("Failed to claim task {0}: status {1}".format(task.id, task_status))
                    
                # Claim went through successfully, commit the claim
                self.conn.commit()
            
        # Keep task object up-to-date
        task.status = 'active'
        task.by_taxi = my_taxi.name


    def write_tasks(self, tasks):
        task_query = """INSERT OR REPLACE INTO tasks
        (id, task_type, depends_on, status, for_taxi, is_recurring, req_time, priority, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        # JSON serialize all tasks
        compiled_tasks = [task.compiled() for task in tasks]
        
        # Build list to insert
        upsert_data = []
        for compiled_task in compiled_tasks:
            task_values = (
                compiled_task['id'],
                compiled_task['task_type'], 
                json.dumps(compiled_task['depends_on'], cls=LocalEncoder),
                compiled_task['status'], 
                compiled_task['for_taxi'] if compiled_task.has_key('for_taxi') else None, 
                compiled_task['is_recurring'],
                compiled_task['req_time'], 
                compiled_task['priority'],
                json.dumps(compiled_task['payload'], cls=LocalEncoder) if compiled_task.has_key('payload') else None,
            )
            upsert_data.append(task_values)
        
        self.execute_update(task_query, *upsert_data)
            
        
    def _get_max_task_id(self):
        task_id_query = """SELECT id FROM tasks ORDER BY id DESC LIMIT 1;"""
        max_id_query = map(dict, self.execute_select(task_id_query))

        if len(max_id_query) == 0:
            return 0
        else:
            return max_id_query[0]['id']
        
        
    def _store_imports(self):
        import_query = """INSERT OR REPLACE INTO imports (import) VALUES (?)"""
        upsert_data = [(ii,) for ii in self.imports]
        self.execute_update(import_query, *upsert_data)


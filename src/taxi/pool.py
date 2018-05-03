#!/usr/bin/env python

# Definition of "Pool" class - manages Taxi attributes and queue status.

import taxi
import time
import os

import traceback

from taxi.batch_queue import RespawnError
import taxi.local.local_queue

class Pool(object):
    """Abstract implementation of a Pool of Taxis. The Pool keeps track of the status
    of individual taxis (i.e., which taxis are running, queued, idled, or have been
    put on hold, died due to some error, or is missing from the queue). The Pool
    also manages submission of taxis, and communicates with a Dispatcher to
    determine which taxis should be running.
    """
    
    taxi_status_codes = [
        'Q',    # queued
        'R',    # running
        'I',    # idle
        'H',    # on hold
        'E',    # error in queue submission
        'M',    # Missing: taxi died unexpectedly
    ]

    def __init__(self, work_dir, log_dir, thrash_delay=300, allocation=None, queue=None):
        self.work_dir = taxi.expand_path(work_dir)
        self.log_dir = taxi.expand_path(log_dir)

        ## thrash_delay sets the minimum time between taxi resubmissions, in seconds.
        ## Default is 5 minutes.
        self.thrash_delay = thrash_delay
        
        # Allocation to run on
        self.allocation = allocation
        
        # Queue to submit to
        self.queue = queue
    

    ### Backend interaction ###
    
    def __enter__(self):
        """Context-manager infrastructure. Pool probably connects to a DB; useful
        to keep connection open for multiple operations, but not leave it open
        constantly."""
        raise NotImplementedError
        
    def __exit__(self):
        """Context-manager infrastructure. Pool probably connects to a DB; useful
        to keep connection open for multiple operations, but not leave it open
        constantly."""
        raise NotImplementedError
    
    def get_all_taxis_in_pool(self):
        """Returns a list of Taxi objects for all taxis in this pool.
        
        The returned taxi objects are not synchronized with the pool; any changes made to them
        will not be reflected in the pool unless they are written back in."""
        raise NotImplementedError

    def get_taxi(self, my_taxi):
        """Retrieves a specific taxi object, provided the name.
        
        The returned taxi object is not synchronized with the pool; any changes made to it
        will not be reflected in the pool unless they are written back in."""
        raise NotImplementedError
    
    def update_taxi_status(self, my_taxi, status):
        """Changes the status for taxi my_taxi (either name or Taxi object) in the
        pool to the provided status."""
        raise NotImplementedError

    def update_taxi_last_submitted(self, my_taxi, last_submit_time):
        """Updates the time when the taxi my_taxi (either name or Taxi object)
        in the pool to the provided time (e.g. time.time())."""
        raise NotImplementedError

    def update_taxi_job_id(self, my_taxi, job_id):
        """Updates the job_id associated with taxi my_taxi (either name or Taxi object)
        in the pool to the provided value."""
        raise NotImplementedError
        
    def update_taxi_current_task(self, my_taxi, current_task):
        """Updates the current_task associated with taxi my_taxi (either name or Taxi object)
        in the pool to the provided value."""
        raise NotImplementedError

    def register_taxi(self, my_taxi):
        """Adds the taxi (Taxi object) my_taxi to the pool.  Also, tells my_taxi which pool
        it is associated with."""
        raise NotImplementedError

    def delete_taxi_from_pool(self, my_taxi):
        """Remove my_taxi from the pool."""
        # Pseudocode:
        # If taxi is a taxi object, extract id.
        # Otherwise, if it's an integer assume it's already an id.
        # Otherwise, complain!

        raise NotImplementedError

    ### Queue interaction ###

    def check_for_thrashing(self, my_taxi):
        """Makes sure my_taxi hasn't been submitted multiple times in quick succession, which
        can quickly burn off an allocation if left unchecked.
        
        'Quick succession' is defined by Pool.thrash_delay, a value in seconds."""
        last_submit = self.get_taxi(my_taxi).time_last_submitted

        if last_submit is None:
            return False
        else:
            current_time = time.time()
            time_since_last_submit = current_time - last_submit
            if time_since_last_submit < self.thrash_delay:
                print "Thrashing detected: {a}-{b} = {c} < {d} (Taxi resubmitted too quickly)".format(a=current_time,
                           b=last_submit, c=time_since_last_submit, d=self.thrash_delay)
            return time_since_last_submit < self.thrash_delay


    def submit_taxi_to_queue(self, my_taxi, queue=None, **kwargs):
        """Submits the Taxi object my_taxi to the batch queue.  If no queue is provided,
        instantiates local_queue.LocalQueue."""
        
        print "LAUNCHING TAXI {0}".format(my_taxi)
        
        if queue is None:
            queue = taxi.local.local_queue.LocalQueue()
        
        # Don't submit hold/error status taxis
        pool_taxi = self.get_taxi(my_taxi)
        taxi_status = pool_taxi.status
        if (taxi_status in ('H', 'E')):
            print "WARNING: did not submit taxi {0} due to status flag {1}.".format(my_taxi, taxi_status)
            return

        if self.check_for_thrashing(my_taxi):
            # Put taxi on hold to prevent thrashing
            print "Thrashing detected for taxi {0}; aborting submission.".format(my_taxi)
            return

        try:
            job_id = queue.launch_taxi(my_taxi, **kwargs)
            self.update_taxi_job_id(my_taxi, job_id)
        except RespawnError as e:
            print str(e) # Don't mark taxis as E if we tried to submit them twice
        except:
            self.update_taxi_status(my_taxi, 'E')
            print "Failed to submit taxi {t}".format(t=str(my_taxi))
            traceback.print_exc()
        
        self.update_taxi_last_submitted(my_taxi, time.time())
        self.update_taxi_queue_status(my_taxi, queue=queue) # 'I' -> 'Q' or 'E', depending on if submission worked


    def remove_taxi_from_queue(self, my_taxi, queue=None):
        """Remove all jobs from the queue associated with my_taxi.  If no queue is provided,
        instantiates local_queue.LocalQueue.
        """
        if queue is None:
            queue = taxi.local.local_queue.LocalQueue()
            
        taxi_status = queue.report_taxi_status(my_taxi)

        for job in taxi_status['job_numbers']:
            queue.cancel_job(job)
            self.update_taxi_job_id(my_taxi, None)


    ### Control logic ###

    def update_all_taxis_queue_status(self, queue=None, dispatcher=None):
        """Looks at the queue and updates the status of all of the taxis in the pool,
        depending on whether the taxis are in the queue running or queued, or absent
        from the queue.
        
        If no queue is provided, instantiates local_queue.LocalQueue.  If no dispatcher
        is provided, won't be able to mark tasks as abandoned when missing taxis are
        detected.
        """
        if queue is None:
            queue = taxi.local.local_queue.LocalQueue()
            
        taxi_list = self.get_all_taxis_in_pool()
        for my_taxi in taxi_list:
            self.update_taxi_queue_status(my_taxi, queue=queue, dispatcher=dispatcher)


    def update_taxi_queue_status(self, my_taxi, queue=None, dispatcher=None):
        """Checks the status of taxi my_taxi in the batch queue, and updates the
        status in the pool.  If no queue is provided, instantiates local_queue.LocalQueue.
        If no dispatcher is provided, cannot mark tasks as abandoned when missing taxis
        are detected.
        
        If a taxi is supposed to be either queued or running, but is absent from the
        batch queue, it is marked missing 'M' in the pool.  If a dispatcher is provided,
        the missing taxi's tasks are marked abandoned.
        """
        
        if queue is None:
            queue = taxi.local.local_queue.LocalQueue()
        queue_status = queue.report_taxi_status(my_taxi)['status']
        
        # Scalability: if my_taxi is a Taxi object, trust the provided status
        if not isinstance(my_taxi, taxi.Taxi):
            pool_status = self.get_taxi(my_taxi).status
        else:
            pool_status = my_taxi.status

        if queue_status in ('Q', 'R'): # Taxi is present on queue
            if pool_status in ('E', 'H'):
                # Hold and error statuses must be changed explicitly
                return
            elif pool_status == 'M':
                # taxi is either no longer missing, or was marked incorrectly
                self.update_taxi_status(my_taxi, queue_status)
                return
            else:
                self.update_taxi_status(my_taxi, queue_status)
                return
        elif queue_status == 'X': # Taxi is not present on queue
            if pool_status in ('E', 'H', 'M', 'I'):
                # Taxi shouldn't be on queue: all is well
                return
            elif pool_status in ('Q', 'R'):
                # Taxi should be on queue, but is MIA
                # NOTE: Implicitly, this means that a currently-running taxi is only allowed to respawn itself
                # i.e., no other taxis are allowed to resubmit a currently-running taxi.
                
                # Mark the taxi as missing
                print "WARNING: Taxi {tn} is missing in action".format(tn=my_taxi)
                self.update_taxi_status(my_taxi, 'M')
                
                # If a taxi is missing, it's probably abandoned a task
                # If provided a dispatcher to talk to, mark that task failed to avoid hanging actives
                if dispatcher is not None:
                    dispatcher.mark_abandoned_task(my_taxi)
                    
                return
        else:
            print "Invalid queue status code - '{0}'".format(queue_status)
            raise BaseException


    def spawn_idle_taxis(self, dispatcher, queue=None):
        """Pool communicates with the provided dispatcher to figure out which taxis
        should be running.  If the dispatcher determines that the workload can support
        additional taxis, it will tell the pool to activate some of its idle taxis.
        If specific taxis are needed to run part of the workload, but they are idle,
        the dispatcher will tell the pool to activate those taxis.
        """
        
        if queue is None:
            queue = taxi.local.local_queue.LocalQueue()
            
        self.update_all_taxis_queue_status(queue=queue, dispatcher=dispatcher)
        
        taxi_list = self.get_all_taxis_in_pool()
        
        # Ask dispatcher which taxis should be running
        with dispatcher:
            should_be_running = dispatcher.should_taxis_be_running(taxi_list)

        # Look through the taxis in the pool, put them in active/inactive states as desired
        for my_taxi in taxi_list:
            if not should_be_running.has_key(str(my_taxi)):
                continue # Dispatcher hasn't given any instructions for this taxi's desired state
                
            if my_taxi.status in ['E', 'H'] and should_be_running[str(my_taxi)]:
                raise Exception("Dispatcher wants taxi {0} to be running, but its state is {1}, which must be changed manually."\
                                .format(my_taxi, my_taxi.status))
                
            elif my_taxi.status in ['Q', 'R'] and not should_be_running[str(my_taxi)]:
                raise NotImplementedError("Dispatcher wants taxi {0} to stop, but it is active.".format(my_taxi))

            # Launch taxi if dispatcher says it should be running            
            if should_be_running[str(my_taxi)]:
                if my_taxi.status == 'I':
                    self.submit_taxi_to_queue(my_taxi, queue=queue)
                elif my_taxi.status not in ['Q', 'R']:
                    raise NotImplementedError("Dispatcher wants taxi {0} running, but its status is {1}, not 'I'"\
                                              .format(my_taxi, my_taxi.status))



import sqlite3
    
    
class SQLitePool(Pool):
    """
    Concrete implementation of the Pool class using an SQLite backend.
    """
    
    def __init__(self, db_path, pool_name=None,
                 work_dir=None, log_dir=None,
                 allocation=None, queue=None,
                 thrash_delay=300,
                 max_connection_attempts=3, retry_sleep_time=10):
        """
        Argument options: either [db_path(, pool_name, thrash_delay)] are specified,
        which specifies the location of an existing pool; or,
        [work_dir, log_dir(, pool_name, thrash_delay)] are specified, which specifies
        where to create a new pool.  If all are specified,
        then [db_path, pool_name] takes priority (e.g., access existing pool
        behavior) and the remaining inputs are ignored.
        
        If no pool_name is provided when accessing an existing DB, and there is only
        one pool in the pool DB, then it accesses that one.  If more than one pool
        is present, must specify pool_name.
        
        If creating a new pool DB, and pool_name is not specified, names the pool
        'default'.
        
        queue specifies which queue/machine to submit to, if relevant
        (e.g., bc or ds on the USQCD machines).
        """
        super(SQLitePool, self).__init__(work_dir=work_dir, log_dir=log_dir,
             thrash_delay=thrash_delay, allocation=allocation, queue=queue)

        self.db_path = taxi.expand_path(db_path)
            
        if not os.path.exists(self.db_path) and pool_name is None:
            # Case: Making a new pool but pool_name not provided.  Set to default.
            self.pool_name = 'default'
        else:
            # Case: Accessing existing pool. If pool name is not provided, and
            # only one pool in DB, use that one.  If pool name is provided, find that one.
            self.pool_name = pool_name

        self.conn = None
        self.max_connection_attempts = max_connection_attempts
        self.retry_sleep_time = retry_sleep_time
        
        self._in_context = False
        
        with self:
            pass # Semi-kludgey creation/retrieval of pool DB
    

    def write_table_structure(self):
        create_taxi_str = """
            CREATE TABLE IF NOT EXISTS taxis (
                name text PRIMARY KEY,
                pool_name text REFERENCES pools (name),
                time_limit real,
                cores integer,
                nodes integer,
                time_last_submitted real,
                status text,
                dispatch text,
                job_id text,
                current_task integer
            )"""
            
        create_no_idle_to_missing_str = """
            CREATE TRIGGER IF NOT EXISTS no_idle_to_missing
            AFTER UPDATE OF status ON taxis
            WHEN (NEW.status = 'M' AND OLD.status IN ('I', 'H'))
            BEGIN
                UPDATE taxis SET status=OLD.status WHERE name=OLD.name;
            END;
            """
            
        create_pool_str = """
            CREATE TABLE IF NOT EXISTS pools (
                name text PRIMARY KEY,
                working_dir text,
                log_dir text,
                allocation text,
                queue text
            )"""

        with self.conn:
            self.conn.execute(create_taxi_str)
            self.conn.execute(create_no_idle_to_missing_str)
            self.conn.execute(create_pool_str)
    
    
    def _get_or_create_pool(self):
        self.write_table_structure()

        # Make sure pool details are written
        if self.pool_name is not None:      
            # Case: Pool name provided
            pool_query = """SELECT * FROM pools WHERE name = ?"""
            this_pool_row = self.execute_select(pool_query, self.pool_name)
        else:
            # Case: pool name not provided for existing pool; use first pool found, if only one in DB
            pool_query = """SELECT * FROM pools"""
            this_pool_row = self.execute_select(pool_query)
            if len(this_pool_row) != 1:
                raise Exception("Must provide pool_name if there are more than one pools in pool DB.  Found {0} != 1.".format(len(this_pool_row)))
            

        if len(this_pool_row) == 0:
            # Case: Pool name provided, but doesn't exist in pools table
            # Did not find this pool in the pool DB; add it
            assert self.work_dir is not None, "Must provide work_dir to make new pool in pool DB"
            assert self.log_dir is not None, "Must provide log_dir to make new pool in pool DB"
            
            pool_write_query = """INSERT OR REPLACE INTO pools
                (name, working_dir, log_dir, allocation, queue)
                VALUES (?, ?, ?, ?, ?)"""
            self.execute_update(pool_write_query, self.pool_name, self.work_dir,
                                self.log_dir, self.allocation, self.queue)
        else:
            # Case: Pool name provided, found this pool in the pool DB:
            # retrieve info about it from DB
            if self.work_dir is None: # Allow overrides
                self.work_dir = this_pool_row[0]['working_dir']
            if self.log_dir is None: # Allow overrides
                self.log_dir = this_pool_row[0]['log_dir']
            self.allocation = this_pool_row[0]['allocation']
            self.queue = this_pool_row[0]['queue']
            
        
    ## Note: definition of "enter/exit" special functions allows usage of the "with" operator, i.e.
    ## with SQLitePool(...) as pool:
    ##      ...code...
    ## This automatically takes care of setup/teardown without any try/finally clause.
    
    def __enter__(self):
        """Context interface: connect to SQLite Pool DB.  If performing multiple operations,
        faster to leave a "connection" open than to open and close it repeatedly; dangerous
        to leave a connection open constantly."""
        # Don't allow layered entry
        if self._in_context:
            return
        self._in_context = True
        
        # Try to connect N times to avoid database locking
        for ii in range(self.max_connection_attempts)[::-1]:
            try:
                self.conn = sqlite3.connect(self.db_path, timeout=30.0) # Creates file if it doesn't exist
                continue
            except sqlite3.OperationalError as err:
                if ii > 0:
                    print "Connection failed. Sleeping {0} seconds and trying again ({1} retries remaining)".format(self.retry_sleep_time, ii)
                    time.sleep(self.retry_sleep_time) # Wait a few seconds f
                else:
                    raise err
        self.conn.row_factory = sqlite3.Row # Row factory for return-as-dict
        
        self._get_or_create_pool() # Also retrieves info about pool from DB, including working dir, so must occur here
        
        taxi.ensure_path_exists(taxi.expand_path(self.work_dir)) # Dig out working directory if it doesn't exist
        taxi.ensure_path_exists(taxi.expand_path(self.log_dir)) # Dig out log directory if it doesn;t exist


    def __exit__(self, exc_type, exc_val, exc_traceback):
        """Context interface: connect to SQLite Pool DB.  If performing multiple operations,
        faster to leave a "connection" open than to open and close it repeatedly; dangerous
        to leave a connection open constantly."""
        self.conn.close()
#        os.chdir(self.backup_cwd) # restore original working directory
        self._in_context = False


    def _create_taxi_object(self, db_taxi):
        """Interface to translate Taxi representation in the DB to a Taxi object.
        """
        db_taxi = dict(db_taxi)
        
        new_taxi = taxi.Taxi()
        new_taxi.rebuild_from_dict(db_taxi)
        new_taxi.pool_path = self.db_path
        new_taxi.log_dir = self.log_dir # Tell taxi where log_dir for this pool is
        
        # Pass on pool-level attributes to taxi for convenience
        new_taxi.allocation = self.allocation
        new_taxi.queue = self.queue
        
        return new_taxi


    def execute_select(self, query, *query_args):
        """Executes a select query on the attached pool DB.
        
        If pool is not in context, opens connection to the pool DB before making the
        query; if only executing one DB operation, this can save writing, but will
        be substantially slower for multiple operations than opening a context.
        
        Often best to do map(dict, ...) on the results."""
        
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
        """Executes a write or update on the attached pool DB.
        
        If pool is not in context, opens connection to the pool DB before making the
        query; if only executing one DB operation, this can save writing, but will
        be substantially slower for multiple operations than opening a context."""
        
        # If we're not in context when this is called, get in context
        if not self._in_context:
            with self:
                res = self.execute_update(query, *query_args)
            return res
        
        try:
            with self.conn:
                self.conn.execute(query, query_args)
                self.conn.commit()
        except:
            print "Failed to execute query: "
            print query
            print "with arguments: "
            print query_args
            raise


    def get_all_taxis_in_pool(self):
        """Queries the pool DB to get all taxis in the pool.
        
        Returns a list of reconstructed taxi objects.
        """

        query = """SELECT * FROM taxis;"""
        taxi_raw_info = self.execute_select(query)
        return [self._create_taxi_object(row) for row in taxi_raw_info]


    def get_taxi(self, my_taxi):
        """Retrieves the taxi my_taxi (specified as either a Taxi object or the
        name of that taxi) from the pool (version in the pool may be different
        than some previously-extracted Taxi object in my_taxi, as my_taxi
        and the representation of that taxi in the pool DB are not synchronized
        and either may be changed)."""
        taxi_name = str(my_taxi)

        query = """SELECT * FROM taxis WHERE name == ?"""
        db_rows = self.execute_select(query, taxi_name)
        
        if len(db_rows) == 0:
            return None
        elif len(db_rows) > 1:
            print map(dict, db_rows)
            raise Exception("Multiple taxis with name=%s found in pool"%taxi_name)
        else:
            this_taxi = self._create_taxi_object(db_rows[0])
            return this_taxi
    
    
    def update_taxi_status(self, my_taxi, status):
        """Updates the status of the taxi my_taxi (Taxi object or name of taxi)
        in the SQLite pool DB."""
        taxi_name = str(my_taxi)

        update_query = """UPDATE taxis SET status = ? WHERE name = ?"""
        self.execute_update(update_query, status, taxi_name)



    def update_taxi_last_submitted(self, my_taxi, last_submit_time):
        """Update when the taxi my_taxi (Taxi object or name of taxi) was last
        submitted in the SQLite pool DB."""
        taxi_name = str(my_taxi)
        
        update_query = """UPDATE taxis SET time_last_submitted = ? WHERE name = ?"""
        self.execute_update(update_query, last_submit_time, taxi_name)
        
    def update_taxi_job_id(self, my_taxi, job_id):
        """Updates the job_id associated with taxi my_taxi (either name or Taxi object)
        in the pool to the provided value."""
        taxi_name = str(my_taxi)
        update_query = """UPDATE taxis SET job_id = ? WHERE name = ?"""
        self.execute_update(update_query, job_id, taxi_name)        

    def update_taxi_current_task(self, my_taxi, current_task):
        """Updates the job_id associated with taxi my_taxi (either name or Taxi object)
        in the pool to the provided value."""
        taxi_name = str(my_taxi)
        update_query = """UPDATE taxis SET current_task = ? WHERE name = ?"""
        self.execute_update(update_query, current_task, taxi_name)  

    def update_taxi_dispatch(self, my_taxi, dispatch_path):
        """Update which dispatch (i.e., the path to the dispatch DB) the taxi
        my_taxi (Taxi object or name of taxi) is associated with in the SQLite pool DB."""
        taxi_name = str(my_taxi)

        update_query = """UPDATE taxis SET dispatch = ? WHERE name = ?"""
        self.execute_update(update_query, dispatch_path, taxi_name)


    def register_taxi(self, my_taxi):
        """
        Register a taxi with the pool.  (Adds to the pool if the taxi is new;
        otherwise, sets taxi pool attributes.) Sets (in the pool DB representation
        of the taxi) which pool the taxi my_taxi is associated.
        
        If no taxi name is provided (i.e., my_taxi.name is not set), a name will automatically be generated like
        {name of pool}{first available integer}, e.g., pg10 for the 10th taxi in pool 'pg'.
        """
        my_taxi.pool_name = self.pool_name
        my_taxi.pool_path = self.db_path
        my_taxi.log_dir = self.log_dir
        
        if getattr(my_taxi, 'name', None) is None:
            # Pop a name off queue of taxi names, for convenience
            my_taxi.name = self.get_next_unused_taxi_name()
        else:
            # Name was specified, make sure there's no collision
            taxi_name_query = """SELECT name FROM taxis;"""
            all_taxi_names = map(lambda t: t['name'], self.execute_select(taxi_name_query))

            if my_taxi.name in all_taxi_names:
                # Already registered in pool DB
                print "Taxi with name {0} already registered in pool {1}/{2}".format(my_taxi.name, self.db_path, self.pool_name)
                return

        self._add_taxi_to_pool(my_taxi)
        
        
    def get_next_unused_taxi_name(self):
        """Convenience function: determines the next available taxi name of the
        form {name of pool}{first available integer}, e.g., pg10 for the 10th
        taxi in pool 'pg'."""
        taxis = self.get_all_taxis_in_pool()
        taxi_names = [t.name for t in taxis]
        
        ii = 0
        while True:
            new_name = "{0}-{1}".format(self.pool_name, ii)
            if new_name not in taxi_names:
                break
            ii += 1
            
        return new_name


    def _add_taxi_to_pool(self, my_taxi):
        """Add (or update) an already-initialized taxi to the pool.
        To be used by register_taxi and potentially
        other helper functions down the road.
        """

        insert_taxi_query = """INSERT OR REPLACE INTO taxis
            (name, pool_name, time_limit, cores, nodes, time_last_submitted, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        self.execute_update(insert_taxi_query, my_taxi.name, my_taxi.pool_name, my_taxi.time_limit, 
            my_taxi.cores, my_taxi.nodes, my_taxi.time_last_submitted, my_taxi.status)


    def delete_taxi_from_pool(self, my_taxi):
        """Delete a particular taxi from the pool.
        """
        taxi_name = str(my_taxi)

        remove_query = """DELETE FROM taxis WHERE name = ?"""

        self.execute_update(remove_query, taxi_name)

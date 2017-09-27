#!/usr/bin/env python

# Definition of "Pool" class - manages Taxi attributes and queue status.

import taxi
import time
import os

import traceback

class Pool(object):

    taxi_status_codes = [
        'Q',    # queued
        'R',    # running
        'I',    # idle
        'H',    # on hold
        'E',    # error in queue submission
        'M',    # Missing: taxi died unexpectedly
    ]

    def __init__(self, work_dir, log_dir, thrash_delay=300, allocation=None):
        self.work_dir = taxi.expand_path(work_dir)
        self.log_dir = taxi.expand_path(log_dir)

        ## thrash_delay sets the minimum time between taxi resubmissions, in seconds.
        ## Default is 5 minutes.
        self.thrash_delay = thrash_delay
        
        # Allocation to run on
        self.allocation = allocation
    

    ### Backend interaction ###
    
    def __enter__(self):
        raise NotImplementedError
        
    def __exit__(self):
        raise NotImplementedError
    
    def get_all_taxis_in_pool(self):
        raise NotImplementedError

    def get_taxi(self, my_taxi):
        raise NotImplementedError
    
    def update_taxi_status(self, my_taxi, status):
        raise NotImplementedError

    def update_taxi_last_submitted(self, my_taxi, last_submit_time):
        raise NotImplementedError

    def register_new_taxi(self, my_taxi):
        raise NotImplementedError

    def delete_taxi_from_pool(self, my_taxi):
        # Pseudocode:
        # If taxi is a taxi object, extract id.
        # Otherwise, if it's an integer assume it's already an id.
        # Otherwise, complain!

        raise NotImplementedError

    ### Queue interaction ###

    def check_for_thrashing(self, my_taxi):
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
            print "Thrashing detected for taxi {0}; set to hold.".format(my_taxi)
            self.update_taxi_status(my_taxi, 'H')
            return

        try:
            queue.launch_taxi(my_taxi, **kwargs)
        except:
            self.update_taxi_status(my_taxi, 'E')
            print "Failed to submit taxi {t}".format(t=str(my_taxi))
            traceback.print_exc()
        
        self.update_taxi_last_submitted(my_taxi, time.time())


    def remove_taxi_from_queue(self, my_taxi, queue=None):
        """Remove all jobs from the queue associated with the given taxi.
        """
        if queue is None:
            queue = taxi.local.local_queue.LocalQueue()
            
        taxi_status = queue.report_taxi_status(my_taxi)

        for job in taxi_status['job_numbers']:
            queue.cancel_job(job)

        return


    ### Control logic ###

    def update_all_taxis_queue_status(self, queue=None, dispatcher=None):
        if queue is None:
            queue = taxi.local.local_queue.LocalQueue()
            
        taxi_list = self.get_all_taxis_in_pool()
        for my_taxi in taxi_list:
            self.update_taxi_queue_status(my_taxi, queue=queue, dispatcher=dispatcher)


    def update_taxi_queue_status(self, my_taxi, queue=None, dispatcher=None):
        if queue is None:
            queue = taxi.local.local_queue.LocalQueue()
            
        queue_status = queue.report_taxi_status(my_taxi)['status']
        pool_status = self.get_taxi(my_taxi).status

        if queue_status in ('Q', 'R'): # Taxi is present on queue
            if pool_status in ('E', 'H', 'M'):
                # Hold and error statuses must be changed explicitly
                return
            else:
                self.update_taxi_status(my_taxi, queue_status)
                return
        elif queue_status == 'X': # Taxi is not present on queue
            if pool_status in ('E', 'H', 'M'):
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
                # Taxi is marked (I)dle, and not present on queue: all is well
                return
        else:
            print "Invalid queue status code - '{0}'".format(queue_status)
            raise BaseException


    def spawn_idle_taxis(self, dispatcher, queue=None):
        if queue is None:
            queue = taxi.local.local_queue.LocalQueue()
            
        self.update_all_taxis_queue_status(queue=queue)
        
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
                 allocation=None, thrash_delay=300):
        """
        Argument options: either [db_path, pool_name(, thrash_delay)] are specified, or
        [work_dir, log_dir(, thrash_delay)] are specified.  If all are specified,
        then [db_path, pool_name] takes priority and the remaining inputs
        are ignored.
        """
        super(SQLitePool, self).__init__(work_dir=work_dir, log_dir=log_dir,
             thrash_delay=thrash_delay, allocation=allocation)

        self.db_path = taxi.expand_path(db_path)
            
        if not os.path.exists(self.db_path) and pool_name is None:
            # Case: Making a new pool but pool_name not provided.  Set to default.
            self.pool_name = 'default'
        else:
            # Case: Accessing existing pool. If pool name is not provided, and
            # only one pool in DB, use that one.  If pool name is provided, find that one.
            self.pool_name = pool_name

        self.conn = None
        
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
                dispatch text
            )"""
        create_pool_str = """
            CREATE TABLE IF NOT EXISTS pools (
                name text PRIMARY KEY,
                working_dir text,
                log_dir text,
                allocation text
            )"""

        with self.conn:
            self.conn.execute(create_taxi_str)
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
                (name, working_dir, log_dir, allocation)
                VALUES (?, ?, ?, ?)"""
            self.execute_update(pool_write_query, self.pool_name, self.work_dir,
                                self.log_dir, self.allocation)
        else:
            # Case: Pool name provided, found this pool in the pool DB:
            # retrieve info about it from DB
            if self.work_dir is None: # Allow overrides
                self.work_dir = this_pool_row[0]['working_dir']
            if self.log_dir is None: # Allow overrides
                self.log_dir = this_pool_row[0]['log_dir']
            self.allocation = this_pool_row[0]['allocation']
            
        
    ## Note: definition of "enter/exit" special functions allows usage of the "with" operator, i.e.
    ## with SQLitePool(...) as pool:
    ##      ...code...
    ## This automatically takes care of setup/teardown without any try/finally clause.
    
    def __enter__(self):
        # Don't allow layered entry
        if self._in_context:
            return
        self._in_context = True
        
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row # Row factory for return-as-dict
        
        self._get_or_create_pool() # Also retrieves info about pool from DB, including working dir, so must occur here
        
        taxi.ensure_path_exists(taxi.expand_path(self.work_dir)) # Dig out working directory if it doesn't exist
        taxi.ensure_path_exists(taxi.expand_path(self.log_dir)) # Dig out log directory if it doesn;t exist

    def __exit__(self, exc_type, exc_val, exc_traceback):
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
        new_taxi.allocation = self.allocation

        return new_taxi


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

        return


    def get_all_taxis_in_pool(self):
        """Queries the pool DB to get all taxis in the pool.
        
        Returns a list of reconstructed taxi objects.
        """

        query = """SELECT * FROM taxis;"""
        taxi_raw_info = self.execute_select(query)
        return [self._create_taxi_object(row) for row in taxi_raw_info]


    def get_taxi(self, my_taxi):
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
        taxi_name = str(my_taxi)

        update_query = """UPDATE taxis SET status = ? WHERE name = ?"""
        self.execute_update(update_query, status, taxi_name)

        return


    def update_taxi_last_submitted(self, my_taxi, last_submit_time):
        taxi_name = str(my_taxi)
        
        update_query = """UPDATE taxis SET time_last_submitted = ? WHERE name = ?"""
        self.execute_update(update_query, last_submit_time, taxi_name)

        return


    def update_taxi_dispatch(self, my_taxi, dispatch_path):
        taxi_name = str(my_taxi)

        update_query = """UPDATE taxis SET dispatch = ? WHERE name = ?"""
        self.execute_update(update_query, dispatch_path, taxi_name)


    def register_taxi(self, my_taxi):
        """
        Register a taxi with the pool.  (Adds to the pool if the taxi is new;
        otherwise, sets taxi pool attributes.)
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
        """Add an already-initialized taxi to the pool.
        To be used by register_new_taxi and potentially
        other helper functions down the road.
        """

        insert_taxi_query = """INSERT OR REPLACE INTO taxis
            (name, pool_name, time_limit, cores, nodes, time_last_submitted, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        self.execute_update(insert_taxi_query, my_taxi.name, my_taxi.pool_name, my_taxi.time_limit, 
            my_taxi.cores, my_taxi.nodes, my_taxi.time_last_submitted, my_taxi.status)

        return


    def delete_taxi_from_pool(self, my_taxi):
        """Delete a particular taxi from the pool.
        """
        taxi_name = str(my_taxi)

        remove_query = """DELETE FROM taxis WHERE name = ?"""

        self.execute_update(remove_query, taxi_name)

        return

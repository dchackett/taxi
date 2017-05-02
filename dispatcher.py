#!/usr/bin/env python

# Definition of "Dispatcher" class - manages task forest and assigns tasks to Taxis.

class Dispatcher(object):

    def __init__(self):
        pass

    


class SQLiteDispatcher(Dispatcher):
    """
    Implementation of the Dispatcher abstract class using SQLite as a backend.
    """ 

    def __init__(self, db_path):
        self.db_path = db_path

        ## Open connection

    
    ## Add destructor when I remember the magic syntax

    
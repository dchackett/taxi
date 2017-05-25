import os


### Job classes
class Job(object):
    def __init__(self, req_time=0, **kwargs):
        self.req_time = req_time
        self.status = 'pending'
        self.is_recurring = False

        # Tree properties
        self.trunk = False
        self.branch_root = False
        self.priority = -1
        
        # Want a default, but don't clobber anything set by a subclass before calling superconstructor
        if not hasattr(self, 'depends_on'):
            self.depends_on = []
        
    def compile(self):
        # Package in to JSON forest format
        self.compiled = {
            'task_type' :   'none',
            'id'        :   self.job_id,
            'req_time'  :   self.req_time,
            'status'    :   self.status,
            'is_recurring': self.is_recurring,
            'priority':     self.priority,
        }
        
        # Get dependencies in job_id format
        if not hasattr(self, 'depends_on') or self.depends_on is None or len(self.depends_on) == 0:
            self.compiled['depends_on'] = None
        else:
            self.compiled['depends_on'] = [d.job_id for d in self.depends_on]


class CopyJob(Job):
    def __init__(self, src, dest, req_time=60, **kwargs):
        super(CopyJob, self).__init__(req_time=req_time, **kwargs)
        
        self.src = src
        self.dest = dest
        
        
    def compile(self):
        super(CopyJob, self).compile()
        
        self.compiled.update({
                'task_type' : 'copy',
                'task_args' : {
                    'src' : self.src,
                    'dest' : self.dest
                }
            })    


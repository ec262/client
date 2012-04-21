import asyncore
import socket
import logging
from protocol import Protocol
from task import MapReduceJob, RepeatedCommandTask
from sandbox import freeze_function
import settings

class Foreman(object):
    def __init__(self):
        self.mapfn = self.reducefn = self.datasource = None
    
    def run(self, workers):
        for worker in workers:
            sc = WorkerController(worker, self)
        asyncore.loop()
        return self.mapreducetasks.result

    def set_datasource(self, ds):
        """Set the data to process and create a new TaskManager for it"""
        self._datasource = ds
        self.mapreducetasks = MapReduceJob(self._datasource, RepeatedCommandTask, repetitions=4)
        self.tasks = iter(self.mapreducetasks)
    
    def get_datasource(self):
        """Get the data that we are processing/will process"""
        return self._datasource

    datasource = property(get_datasource, set_datasource)

class WorkerController(Protocol):
    def __init__(self, worker, server):
        """Connect to the specified worker"""
        Protocol.__init__(self)
        self.server = server
        self.register_command('taskcomplete', self.complete_task)
        self.register_command('ready', lambda x, y: self.initialize_worker())
        # Create connection
        logging.debug("Connecting to worker %s:%d..." % worker)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(worker)

    def initialize_worker(self):
        """Upon connecting, send the map/reduce functions and start a task"""
        if self.server.mapfn:
            self.send_command('mapfn', freeze_function(self.server.mapfn))
        if self.server.reducefn:
            self.send_command('reducefn', freeze_function(self.server.reducefn))
        self.start_new_task()
            
    def handle_close(self):
        """Override default close handler"""
        logging.info("Client disconnected")
        self.close()
        
    def start_new_task(self):
        """Ask the TaskManager what to do next and pass message to worker"""
        task = self.server.tasks.next()
        if task == None:
            logging.debug('No tasks to perform')
            self.handle_close()
        self.task = task
        task.add_worker(self)

    def complete_task(self, command, data):
        """Recieve the results of a map task"""
        self.task.complete(self, data)
        self.task = None
        self.start_new_task()
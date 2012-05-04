import asyncore
import socket
import logging
from protocol import Protocol
from task import MapReduceJob, RepeatedCommandTask
from sandbox import freeze_function
import settings
from discovery import get_tasks

class Foreman(object):
    def __init__(self):
        self.mapfn = self.reducefn = self.datasource = None
        self.chunks = {}
    
    def run(self, workers):
        for task in self.mapreducetasks:
            response = get_tasks(1)
            if len(response) == 0:
                logging.info("No workers found")
                time.sleep(5) # wait and hope that more workers join
            else:
                task_id, workers = response.iteritems().next()
                task.id = task_id
                for worker in workers:
                    host, port = worker.split(':')
                    worker = (host, int(port))
                    WorkerController(worker, task, task_id, self)
        asyncore.loop()
        return self.mapreducetasks.result

    def set_datasource(self, ds):
        """Set the data to process and create a new TaskManager for it"""
        self._datasource = ds
        self.mapreducetasks = MapReduceJob(self._datasource, RepeatedCommandTask, repetitions=3)
    
    def get_datasource(self):
        """Get the data that we are processing/will process"""
        return self._datasource

    datasource = property(get_datasource, set_datasource)


class WorkerController(Protocol):
    def __init__(self, worker, task, server):
        """Connect to the specified worker"""
        Protocol.__init__(self)
        self.task = task
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
        self.start_task()
        
    def handle_close(self):
        """Override default close handler"""
        logging.info("Client disconnected")
        self.close()
    
    def start_task(self):
        """Start the task that the worker was assigned to at initialization"""
        self.task.add_worker(self)

    def complete_task(self, command, data):
        """Recieve the results of a map task"""
        self.task.complete(self, data)
        self.task = None
        logging.info("Completed task")
        self.close()
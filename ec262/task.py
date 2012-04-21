import itertools
import random
import uuid

class DataChunker(object):
    """Class that allows us to iterate through data with dynamic chunking"""
    
    def __init__(self, datasource, rows=1):
        self.data = datasource
        self.rows = rows
        self.done = False
    
    def set_rows(self, rows):
        """Set the number of rows to send at a time"""
        self.rows = rows
    
    def __iter__(self):
        """Iterator for returning data chunks of the appropriate length"""
        it = self.data.iteritems()
        data = tuple(itertools.islice(it, self.rows))
        while len(data) > 0:
            yield data
            data = tuple(itertools.islice(it, self.rows))
        self.done = True


class Task(object):
    """A single task that can be performed"""
    WAITING = 0
    RUNNING = 1
    COMPLETE = 2
    
    def __init__(self, *args, **kwargs):
        self.id = uuid.uuid4()
        self._state = None
        self.state = Task.WAITING
        self.result = None
        self.workers = set()
    
    def add_worker(self, worker):
        """Add a worker to work on the task"""
        self.workers.add(worker)
        self.handle_worker(worker)
        if self.is_running():
            self.state = Task.RUNNING
    
    def complete(self, worker, result):
        """Mark the task as complete"""
        if self.state != Task.COMPLETE and self.is_complete(worker, result):
            self.result = (worker, result)
            self.state = Task.COMPLETE
    
    def set_state(self, state):
        if self._state != state:
            self._state = state
            if state == Task.WAITING:
                self.handle_waiting()
            elif state == Task.RUNNING:
                self.handle_running()
            elif state == Task.COMPLETE:
                self.handle_complete()
    
    def get_state(self):
        return self._state
    
    state = property(get_state, set_state)
    
    def is_running(self):
        """Test to see if the task is currently running"""
        return len(self.workers) > 0
    
    def is_complete(self, worker, result):
        """Test to see if the task has been completed"""
        return True
    
    def handle_worker(self, worker):
        pass
    def handle_waiting(self):
        pass
    def handle_running(self):
        pass
    def handle_complete(self):
        pass


class CommandTask(Task):
    """A task to run the given command"""
    
    def __init__(self, command, data=None, *args, **kwargs):
        Task.__init__(self, *args, **kwargs)
        self.command = command
        self.data = data
    
    def handle_worker(self, worker):
        """Run the command by having the worker send it out"""
        print "SEND_COMMAND: ", self.command, self.data
        worker.send_command(self.command, self.data)
        

class RepeatedTask(Task):
    def __init__(self, repetitions=1, *args, **kwargs):
        Task.__init__(self, *args, **kwargs)
        self.repetitions = repetitions
        self.task_workers = {}
        self.results = {}
    
    def handle_worker(self, worker):
        if len(self.task_workers) < self.repetitions:
            rep = len(self.task_workers)
        else:
            # Ideas: random task, task w/ fewest workers
            rep = random.randrange(self.repetitions)
        if rep not in self.task_workers:
            self.task_workers[rep] = set()
        self.task_workers[rep].add(worker)
        self.handle_repeated_worker(worker, rep)
    
    def is_running(self):
        return len(self.workers) == self.repetitions
    
    def complete(self, worker, result):
        for rep in self.task_workers:
            if worker in self.task_workers[rep] and rep not in self.results:
                self.results[rep] = result
                break
        if self.state != Task.COMPLETE and self.is_complete(worker, result):
            self.result = self.merge_results()
            self.state = Task.COMPLETE
    
    def is_complete(self, worker, result):
        return len(self.results) == self.repetitions
    
    def merge_results(self):
        return self.results[0]
    
    def handle_repeated_worker(worker, rep):
        pass


class RepeatedCommandTask(RepeatedTask, CommandTask):
    def __init__(self, *args, **kwargs):
        CommandTask.__init__(self, *args, **kwargs)
        RepeatedTask.__init__(self, *args, **kwargs)
        
    def handle_repeated_worker(self, worker, rep):
        CommandTask.handle_worker(self, worker)

    def handle_complete(self):
        pass


class Job(object):
    def __init__(self, data, TaskClass, **kwargs):
        self.data = data
        self.TaskClass = TaskClass
        self.kwargs = kwargs
        self.result = None
    
    def __iter__(self):
        dc = DataChunker(self.data)
        tasks = [self.TaskClass(data=data, **self.kwargs) for data in dc]
        while any([t.state == Task.WAITING for t in tasks]):
            for t in filter(lambda t: t.state == Task.WAITING, tasks):
                yield t
        while any([t.state != Task.COMPLETE for t in tasks]):
            for t in filter(lambda t: t.state != Task.COMPLETE, tasks):
                yield t
        self.result = self.merge_results([t.result for t in tasks])
        
    def merge_results(self, results):
        print "HERE"
        return results


class MapReduceJob(Job):
    def __init__(self, data, TaskClass, **kwargs):
        Job.__init__(self, data, TaskClass, **kwargs)
    
    def __iter__(self):
        mapjob = Job(self.data, self.TaskClass, command='map', **self.kwargs)
        mapjob.merge_results = self.merge_map_results
        print mapjob.merge_results
        for t in mapjob:
            yield t
        reducejob = Job(mapjob.result, self.TaskClass, command='reduce', **self.kwargs)
        reducejob.merge_results = self.merge_reduce_results
        for t in reducejob:
            yield t
        self.result = reducejob.result
        while True:
            yield CommandTask('disconnect')
    
    def merge_map_results(self, results):
        output = {}
        for data in results:
            for key, values in data.iteritems():
                if key not in output:
                    output[key] = ()
                output[key] += values
        return output
    
    def merge_reduce_results(self, results):
        print results
        output = {}
        for data in results:
            for key, value in data.iteritems():
                output[key] = value
        return output
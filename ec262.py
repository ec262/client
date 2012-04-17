#!/usr/bin/env python


################################################################################
# Copyright (c) 2010 Michael Fairley
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
################################################################################

import asynchat
import asyncore
import cPickle as pickle
import hashlib
import hmac
import logging
import marshal
import optparse
import os
import random
import socket
import sys
import types
import uuid
import operator
import itertools
import pprint

pp = pprint.PrettyPrinter(indent=2)

VERSION = "0.1.2"


DEFAULT_PORT = 11235


class Protocol(asynchat.async_chat):
    def __init__(self, conn=None):
        if conn:
            asynchat.async_chat.__init__(self, conn)
        else:
            asynchat.async_chat.__init__(self)

        self.set_terminator("\n")
        self.buffer = ""
        self.mid_command = None

    def collect_incoming_data(self, data):
        """Receive data and append it to an internal buffer"""
        self.buffer += data

    def send_command(self, command, data=None):
        """Send command and optional data over connection
        
        * `command` is a string without colons and newlines; a colon is always
          appended to it
        * If `data` is specified, then it will be pickled and appended to 
          `command`.
        """
        command += ':'
        if data:
            pdata = pickle.dumps(data)
            self.push(command + str(len(pdata)) + '\n' + pdata)
        else:
            self.push(command + "\n")

    def found_terminator(self):
        if not self.mid_command:
            command, data_length = self.buffer.split(":", 1)
            if data_length:
                self.set_terminator(int(data_length))
                self.mid_command = command
            else:
                self.process_command(command)
        else:
            data = pickle.loads(self.buffer)
            self.set_terminator("\n")
            command = self.mid_command
            self.mid_command = None
            self.process_command(command, data)
        self.buffer = ""

    def process_command(self, command, data=None):
        commands = {
            'disconnect': lambda x, y: self.handle_close(),
        }
        
        if command in commands:
            commands[command](command, data)
        else:
            logging.critical("Unknown command received: %s" % (command,)) 
            self.handle_close()


class Client(Protocol):
    def __init__(self):
        Protocol.__init__(self)
        self.mapfn = self.reducefn = None
        self.uuid = uuid.uuid4()
        
    def conn(self, server, port):
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((server, port))
        asyncore.loop()
    
    def handle_connect(self):
        self.send_command('identify', self.uuid)

    def handle_close(self):
        self.close()

    def set_mapfn(self, command, mapfn):
        self.mapfn = types.FunctionType(marshal.loads(mapfn), globals(), 'mapfn')

    def set_reducefn(self, command, reducefn):
        self.reducefn = types.FunctionType(marshal.loads(reducefn), globals(), 'reducefn')

    def call_mapfn(self, command, data):
        logging.info("Mapping %s" % str(data[0]))
        results = {}
        for k, v in self.mapfn(data[0], data[1]):
            if k not in results:
                results[k] = []
            results[k].append(v)
        self.send_command('mapdone', (data[0], results))

    def call_reducefn(self, command, data):
        logging.info("Reducing %s" % str(data[0]))
        results = self.reducefn(data[0], data[1])
        self.send_command('reducedone', (data[0], results))
        
    def process_command(self, command, data=None):
        commands = {
            'mapfn': self.set_mapfn,
            'reducefn': self.set_reducefn,
            'map': self.call_mapfn,
            'reduce': self.call_reducefn,
        }

        if command in commands:
            commands[command](command, data)
        else:
            Protocol.process_command(self, command, data)


class Server(asyncore.dispatcher, object):
    def __init__(self):
        asyncore.dispatcher.__init__(self)
        self.mapfn = None
        self.reducefn = None
        self.datasource = None

    def run_server(self, port=DEFAULT_PORT):
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind(("", port))
        self.listen(1)
        try:
            asyncore.loop()
        except:
            self.close_all()
            raise
        return self.taskmanager.results

    def handle_accept(self):
        conn, addr = self.accept()
        sc = ServerChannel(conn, self)

    def handle_close(self):
        self.close()

    def set_datasource(self, ds):
        self._datasource = ds
        self.taskmanager = TaskManager(self._datasource, self)
    
    def get_datasource(self):
        return self._datasource

    datasource = property(get_datasource, set_datasource)


class ServerChannel(Protocol):
    def __init__(self, conn, server):
        Protocol.__init__(self, conn)
        self.server = server
        self.uuid = None

    def handle_close(self):
        logging.info("Client disconnected")
        self.close()

    def start_new_task(self):
        command, data = self.server.taskmanager.next_task(self)
        if command == None:
            return
        self.send_command(command, data)

    def map_done(self, command, data):
        self.server.taskmanager.map_done(self, data)
        self.start_new_task()

    def reduce_done(self, command, data):
        self.server.taskmanager.reduce_done(data)
        self.start_new_task()

    def identify(self, command, data):
        self.uuid = data
        self.init_process()
    
    def process_command(self, command, data=None):
        commands = {
            'identify': self.identify,
            'mapdone': self.map_done,
            'reducedone': self.reduce_done,
        }
        if command in commands:
            commands[command](command, data)
        else:
            Protocol.process_command(self, command, data)
    
    def init_process(self):
        if self.server.mapfn:
            self.send_command('mapfn', marshal.dumps(self.server.mapfn.func_code))
        if self.server.reducefn:
            self.send_command('reducefn', marshal.dumps(self.server.reducefn.func_code))
        self.start_new_task()

class TaskManager:
    START = 0
    MAPPING = 1
    REDUCING = 2
    FINISHED = 3

    def __init__(self, datasource, server):
        self.datasource = datasource
        self.server = server
        self.state = TaskManager.START
        self.copies = 3
        self.running_tasks = None
        self.worker_map = None

    def run_task(self, worker, command, data, instance_num):
        if (data, instance_num) not in self.running_tasks[command]:
            self.running_tasks[command][(data, instance_num)] = set()
        self.running_tasks[command][(data, instance_num)].add(worker.uuid.hex)
        self.worker_map[worker.uuid] = (data, instance_num)
        return (command, data)
    
    def run_map(self, worker, next_state):
        task = self.get_task('map', next_state)
        if task is not None:
            data, instance_num = task
            return self.run_task(worker, 'map', data, instance_num)

    def get_task(self, command, next_state):
        try:
            key, instance_num = self.data_iters[command].next()
            value = self.datasource[key]
            return (key, value), instance_num
        except StopIteration:
            if len(self.running_tasks[command]) > 0:
                data, instance_num = random.choice(self.running_tasks[command].keys())
                return data, instance_num
            self.state = next_state
            return None

    def verify_task(self, command, handler, worker, data):
        if worker.uuid not in self.worker_map:
            return
        task_data, instance_num = self.worker_map[worker.uuid]
        if task_data[0] != data[0]:
            return
        if (task_data, instance_num) not in self.running_tasks[command]:
            return
        del self.running_tasks[command][(task_data, instance_num)]
        
        # Keep track of finished tasks
        key, task_output = data
        if key not in self.finished_tasks[command]:
            self.finished_tasks[command][key] = {}
        self.finished_tasks[command][key][instance_num] = task_output
        
        # Wait until all copies have completed
        if len(self.finished_tasks[command][key]) == self.copies:
            votes = {}
            hashes = {}
            # Tabulate votes
            for instance_num in self.finished_tasks[command][key]:
                h = hash(frozenset(self.finished_tasks[command][key][instance_num]))
                if h not in votes:
                    votes[h] = 0
                    hashes[h] = self.finished_tasks[command][key][instance_num]
                votes[h] += 1
            # Find majority vote
            majority_vote = max(votes.iteritems(), key=operator.itemgetter(1))[0]
            if votes[majority_vote] > self.copies / 2:
                output = hashes[majority_vote]
                handler(output)

    def next_task(self, worker):
        if self.state == TaskManager.START:
            self.data_iters = {
                'map': itertools.product(iter(self.datasource), range(self.copies))
            }
            self.worker_map = {}
            self.running_tasks = {
                'map': {}
            }
            self.finished_tasks = {
                'map': {}
            }
            
            self.map_results = {}
            self.working_reduces = {}
            self.results = {}
            self.state = TaskManager.MAPPING
        
        if self.state == TaskManager.MAPPING:
            command = self.run_map(worker, TaskManager.REDUCING)
            if command is not None:
                return command
            self.reduce_iter = self.map_results.iteritems()
        
        if self.state == TaskManager.REDUCING:
            try:
                reduce_item = self.reduce_iter.next()
                self.working_reduces[reduce_item[0]] = reduce_item[1]
                return ('reduce', reduce_item)
            except StopIteration:
                if len(self.working_reduces) > 0:
                    key = random.choice(self.working_reduces.keys())
                    return ('reduce', (key, self.working_reduces[key]))
                self.state = TaskManager.FINISHED
        if self.state == TaskManager.FINISHED:
            self.server.handle_close()
            return ('disconnect', None)
    
    def map_done(self, worker, data):
        def handler(data):
            print "Handling!", data
            for (key, values) in data.iteritems():
                if key not in self.map_results:
                    self.map_results[key] = []
                self.map_results[key].extend(values)
        self.verify_task('map', handler, worker, data)
                                
    def reduce_done(self, data):
        # Don't use the results if they've already been counted
        if not data[0] in self.working_reduces:
            return
        self.results[data[0]] = data[1]
        del self.working_reduces[data[0]]

def run_client():
    parser = optparse.OptionParser(usage="%prog [options]", version="%%prog %s"%VERSION)
    parser.add_option("-P", "--port", dest="port", type="int", default=DEFAULT_PORT, help="port")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true")
    parser.add_option("-V", "--loud", dest="loud", action="store_true")

    (options, args) = parser.parse_args()
    
    if options.verbose:
        logging.basicConfig(level=logging.INFO)
    if options.loud:
        logging.basicConfig(level=logging.DEBUG)

    client = Client()
    client.conn(args[0], options.port)


MAPPER = None
REDUCER = None
def mapper(f):
    global MAPPER
    MAPPER = f
    return f

def reducer(f):
    global REDUCER
    REDUCER = f
    return f

def run_job(datasource, port=DEFAULT_PORT):
    s = Server()
    s.datasource = datasource
    s.mapfn = MAPPER
    s.reducefn = REDUCER
    return s.run_server(port=port)

if __name__ == '__main__':
    run_client()

import asyncore, asynchat
import socket
import logging
from protocol import Protocol
from sandbox import unfreeze_and_sandbox_function
import settings
from discovery import register_worker, encrypt_data
import threading
import time

class Server(asyncore.dispatcher):
    def __init__(self):
        asyncore.dispatcher.__init__(self)
        self.port = None
    
    def run(self, port=settings.DEFAULT_PORT):
        """Run the worker server on the given port and wait for a foreman"""
        self.port = port
        
        # Start the hearbeat thread
        self.heartbeat_thread = threading.Thread(target=self.heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        # Create the server
        logging.debug("Starting server on %d" % (port,))
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind(("", port))
        self.listen(1)
        try:
            asyncore.loop()
        except:
            self.close_all()
            raise
        logging.debug("Shutting down server")
    
    def handle_accept(self):
        """When connected to, create a new Worker"""
        conn, addr = self.accept()
        logging.debug("Accepting job from %s:%s" % addr)
        Worker(conn)
    
    def heartbeat(self):
        """Register with the discovery service every 30sec"""
        while True:
            register_worker(self.port)
            time.sleep(30) # timeout = 60sec, heartbeat every 30sec


class Worker(Protocol):
    def __init__(self, conn):
        Protocol.__init__(self, conn)
        self.mapfn = self.reducefn = None
        # Register commands
        self.register_command('mapfn', self.set_mapfn)
        self.register_command('reducefn', self.set_reducefn)
        self.register_command('map', self.call_mapfn)
        self.register_command('reduce', self.call_reducefn)
        # Let foreman know that we're ready
        self.send_command('ready')
    
    def handle_close(self):
        """Override default close handler"""
        logging.debug('Worker disconnect')
        self.close()
    
    def set_mapfn(self, command, mapfn):
        """Set the map function using the given code"""
        self.mapfn = unfreeze_and_sandbox_function(mapfn, 'mapfn')

    def set_reducefn(self, command, reducefn):
        """Set the reduce function using the given code"""
        self.reducefn = unfreeze_and_sandbox_function(reducefn, 'reducefn')

    def call_mapfn(self, command, data):
        """Run the map function on the given key-value pairs
        
        Expects data to be {'task_id': 123, 'map_data': []}
        """
        logging.info("Mapping %s..." % (repr(data)[:30]))
        task_id = data['task_id']
        results = {}
        for row in data['mapr_data']:
            key, value = row
            output = self.mapfn(key, value)
            for key, value in output:
                if key not in results:
                    results[key] = ()
                results[key] += (value,)
        self.send_command('taskcomplete', encrypt_data(results, task_id))
       

    def call_reducefn(self, command, data):
        """Run the reduce function on the given key-values pairs
        
        Expects data to be {'task_id': 123, 'reduce_data': []}
        """
        logging.info("Reducing %s" % repr(data)[:30])
        task_id = data['task_id']
        results = {}
        for row in data['reduce_data']:
            key, values = row
            output = self.reducefn(key, values)
            for key, value in output:
                results[key] = value
        self.send_command('taskcomplete', encrypt_data(results, task_id))
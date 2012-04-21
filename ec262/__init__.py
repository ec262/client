from foreman import Foreman
from worker import Server
from settings import DEFAULT_PORT, VERSION
import sys
import optparse
import logging

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

def run_job(data, workers = None):
    if workers is None:
        workers = [("localhost", DEFAULT_PORT)]
    f = Foreman()
    f.mapfn = MAPPER
    f.reducefn = REDUCER
    f.datasource = data
    return f.run(workers)

def run_worker(port=DEFAULT_PORT):
    parser = optparse.OptionParser(usage="%prog [options]", version="%%prog %s"%VERSION)
    parser.add_option("-P", "--port", dest="port", type="int", default=DEFAULT_PORT, help="port")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true")
    parser.add_option("-V", "--loud", dest="loud", action="store_true")

    (options, args) = parser.parse_args()
    
    if options.verbose:
        logging.basicConfig(level=logging.INFO)
    if options.loud:
        logging.basicConfig(level=logging.DEBUG)

    s = Server()
    s.run(port=options.port)
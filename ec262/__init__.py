from foreman import Foreman
from worker import Server
from settings import DEFAULT_PORT, VERSION

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
    s = Server()
    s.run(port=port)
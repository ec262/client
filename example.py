#!/usr/bin/env python
from ec262 import mapper, reducer, run_job

data = ["Humpty Dumpty sat on a wall",
        "Humpty Dumpty had a great fall",
        "All the King's horses and all the King's men",
        "Couldn't put Humpty together again",
        ]
# The data source can be any dictionary-like object
datasource = dict(enumerate(data))

@mapper
def mapfn(k, v):
    import time
    for w in v.split():
        #time.sleep(1)
        yield w, 1

@reducer
def reducefn(k, vs):
    result = sum(vs)
    return result

results = run_job(datasource, password="changeme")
print results
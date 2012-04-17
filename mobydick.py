#!/usr/bin/env python
from ec262 import mapper, reducer, run_job

@mapper
def mapfn(k, v):
    for w in v.split():
        yield w, 1

@reducer
def reducefn(k, vs):
    result = sum(vs)
    return (k, result)


if __name__ == '__main__':
    f = open('mobydick.txt', 'r')
    datasource = dict(enumerate(f.readlines()))
    results = run_job(datasource)
    print results
EC262 Client Libraries
======================

Introduction
------------

The EC262 Client Libraries contain both Foreman and Worker code. After finding 
workers through the discovery service, the Foreman sends pickled map and reduce
functions and then data to mappers and reducers.

This is built on top of a lightweight [Python](http://www.python.org/) MapReduce 
implementation called [Mincemeat](https://github.com/michaelfairley/mincemeatpy).
In Mincemeat, the workers request jobs from the foreman, and our system is the
the opposite. Once workers registered with the discovery service, the foreman
contacts them with jobs. Other major modifications to Mincemeat include
sandboxing, cheater detection, discovery service integration, encryption
for fairness in the credit system, and significant code refactoring.

Quick start
-----------

### Foreman

To run a job, import the `ec262` module. You can then use the `ec262.mapper`
and `ec262.reducer` decorator to specify your map/reduce functions like so:
    
```python
@ec262.mapper
def mymap(key, value):
    yield (key, value)

@ec262.reducer
def myreduce(key, values):
    yield (key, sum(values))
```

Finally, to run your MapReduce job, call `ec262.run_job(data)`, where `data`
is a dictionary mapping each key to a value.

```python
mapreduce_data = {
    'hello': 1,
    'world': 2
}
ec262.run_job(mapreduce_data)
```

See example.py for more information; it's a working script that counts the
number of times each word appears in "Humpty Dumpty".

### Worker

To start up a worker, run worker.py. You can specify the port you want it to
run on using the `-P PORT` flag (defaults to 11235), and you can have it run
in either verbose mode (`-v`) or loud mode (`-V`).

Example (verbose mode, running on port 12345):

    python worker.py -v -P 12345

You can also run it by importing `ec262` from a script and then calling
`ec262.run_worker([port=11235])`.


Design decisions
----------------

* All data is sent around in dictionaries, which is serialized to JSON as a
  list of [key, value] lists sorted by key. These are encrypted using AES-128
  in cipher-block chaining (CBC) mode, and finally serialized using Base64.

* Sandboxing is done by disallowing potentially dangerous builtin functions
  and whitelisting modules.

* Specification of map and reduce functions has been changed to use decorators.

* Client processes are currently differentiated by UUIDs.


Unexpected challenges
---------------------

* **Integrating with the discovery service.** The teams had slightly different
  ideas about how the protocol worked, arising from the introduction of
  credits. Originally, the client library developers assumed that foreman were
  leased workers for the entire job, which enabled some optimizations. But
  because workers need to get paid for every task, the foreman can only
  ask workers to compute the one task that they're assigned.

* **Encryption and checking results.** Because results are encoded as a
  dictionary, keys can be in arbitrary order. We need to enforce that they are
  always in the same order, so that all clients serialize them same way and 
  the results can be easily checked for correctness. We now encode results in
  JSON as a list of ['key', 'value'] lists, sorted by key. (These results are
  then encrypted with AES, and serialized with Base64.)

* **Testing.** Because keys are destroyed when a foreman requests them, it was
  somewhat tricky to figure out to test the code that gets a key from the
  discovery service to decrypt a task. We ended up breaking that function into
  two relatively simple pieces (getting a key, decrypting the data) and just
  testing those pieces separately. We also tried decrypting data that was
  encrypted with a bogus key, and simply made sure that the right exception
  was thrown during decryption.
  

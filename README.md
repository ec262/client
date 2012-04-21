# EC2(62) Foreman/Worker

## Foreman
To run a job, import the `ec262` module. You can then use the `ec262.mapper` and `ec262.reducer` decorator to specify your map/reduce functions like so:

    @ec262.mapper
    def mymap(key, value):
      yield (key, value)

    @ec262.reducer
    def myreduce(key, values):
      yield (key, sum(values))

Finally, to run your MapReduce job, call `ec262.run_job(data)`, where `data` is a dictionary mapping each key to a value.

    mapreduce_data = {
      'hello': 1,
      'world': 2
    }
    ec262.run_job(mapreduce_data)

See example.py for more information; it's a working script that counts the number of times each word appears in "Humpty Dumpty".

## Worker
To start up a worker, run worker.py. You can specify the port you want it to run on using the `-P PORT` flag, and you can have it run in either verbose mode (`-v`) or loud mode (`-V`).

Example (verbose mode, running on port 12345):

    python worker.py -v -P 12345
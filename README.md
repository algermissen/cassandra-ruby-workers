cassandra-ruby-workers
======================

Coordinate concurrent worker Ruby processes using Cassandra CAS. Uses the new Datastax
Cassandra driver for Ruby:

    https://github.com/datastax/ruby-driver

The cassandra-ruby-workers project explores various possibilities to use Cassandra to coordinate
concurrent workers.

# Installation

Install the Datastax Cassandra Ruby driver. Currently this works best by downloading the
source from github and installing the gem from these sources directly.


Place the script `worker.rb` in any directory on any number of hosts and start the script:

    $ woker.rb --host <ip> --keyspace <keyspace> --workstate <workstate-name>

# The Scripts

- workers.rb: Simple coordination of workers that need to do some work sequentially on a shared resource. Each work progress starts from a shared state and returns the next value for the shared state as a result of the work progress. The workers are not schedule, but start work as soon as they akquire the lock. 

# TODO

- Add timers to trigger the worker periodically to touch lock (to avoid TTL expirery during work)


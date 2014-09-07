cassandra-ruby-workers
======================

Coordinate concurrent worker Ruby processes using Cassandra CAS. Uses the new Datastax
[Cassandra driver for Ruby](https://github.com/datastax/ruby-driver)

The cassandra-ruby-workers project explores various possibilities to use Cassandra to coordinate
concurrent workers.

Especially I am interested in the [asynchronous query execution feature](https://github.com/datastax/ruby-driver/tree/master/features/asynchronous_io) of the new driver.



# Installation

Install the Datastax Cassandra Ruby driver. Currently this works best by downloading the
source from github and installing the gem from these sources directly.


Place any of the worker scripts in any directory on any number of hosts and provide a Cassandra host and keyspace as
well as a workspace name for the worker coordination.

    $ woker.rb --host <ip> --keyspace <keyspace> --workspace <orkspace-name>


# The Scripts

- sequential_counting_worker.rb: Simple coordination of workers that need to do some work sequentially on a shared resource. Each work progress starts from a shared state and returns the next value for the shared state as a result of the work progress. The workers are not scheduled, but start work as soon as they akquire the lock. 
- ...

# Worker Patterns

This project aims to explore the different use cases for concurrent workers (programs that poll for their
work rather then being triggered) and how to solve them with coordination patterns solely based on Cassandra's 
CAS or per-row mutation isolation fueature.

The following patterns have emerged to date

- Sequential workers with the desired for multiple workers for pure 'failover' reasons
- Concurrent workers to distribute workload across processes
- ... more to come


# TODO

- Proper exception handling and recovery strategies to maintain a consistent state of overall work progress
- Add timers to trigger the worker periodically to touch lock (to avoid TTL expiry during work)




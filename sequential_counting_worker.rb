#!/usr/bin/env ruby
require 'net/http'
require 'cassandra'
require 'optparse'
require 'logger'
require_relative 'worker'

host='127.0.0.1'
keyspace='foo'
workspace = 'test'

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: example.rb [options]"

  opts.on("-v", "--[no-]verbose", "Run verbosely") { |v| options[:verbose] = v }
  opts.on("-H", "--host HOST", "Cassandra host") { |h| host = h }
  opts.on("-K", "--keyspace KEYSPACE", "Keyspace to use") { |k| keyspace = k }
  opts.on("-W", "--workspace WORKSPACE", "Name of the workspace to use") { |w| workspace = w }
end.parse!


class SequentialCountingWorker < Worker

  def initialize(host, keyspace, workspace_id)
   super(host, keyspace, workspace_id)
   @sigterm_received = false
  end

  # Do some work that returns a new state *after* the work
  def do_work_and_determine_new_state(state)
    logger.info("Starting work with state=#{state}")
    s = state.to_i
    for i in 1..10
      sleep 1
      s += 1
      logger.info("   ... working, state now #{s}")
    end
    logger.info("Finished work, next state is #{s}")
    return s.to_s 
  end

  def run

    Signal.trap("INT") {
      trap_sigterm
    }

    loop do
      if(!get_lock)
        #logger.info("Unable to get lock")
        sleep 1
        next
      end


      logger.info("Akquired lock")

      state = get_state || ''
      logger.info("Workstate state is #{state}")

      new_state = do_work_and_determine_new_state(state)

      set_state(new_state)
      if(!free_lock)
        # FIXME: What is the correct way to handle this logical error? 
        puts "Error: unable to free lock"
      end
      if(@sigterm_received)
        logger.info("Exiting")
        exit
      end

    end
  end

  def trap_sigterm
    # https://github.com/steveklabnik/mono_logger
    # logger.info("Preparing shutdown...")
    @sigterm_received = true
  end

end


SimpleWorker.new(host,keyspace,workspace).run


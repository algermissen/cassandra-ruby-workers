#!/usr/bin/env ruby
require 'net/http'
require 'cassandra'
require 'optparse'
require 'logger'

logger = Logger.new(STDERR)
logger.level = Logger::INFO

host='127.0.0.1'
keyspace='foo'
workstate_id = 'test'

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: example.rb [options]"

  opts.on("-v", "--[no-]verbose", "Run verbosely") { |v| options[:verbose] = v }
  opts.on("-H", "--host HOST", "Cassandra host") { |h| host = h }
  opts.on("-K", "--keyspace KEYSPACE", "Keyspace to use") { |k| keyspace = k }
  opts.on("-W", "--workstate WORKSTATE", "Name of the workstate to use") { |w| workstate_id = w }
end.parse!

# For some reason, timeuuid makes problems in the perpared query, use string for now
worker_id = Cassandra::TimeUuid::Generator.new.next.to_s

logger.info("Started worker #{worker_id}")
logger.info("Host:         #{host}")
logger.info("Keyspace:     #{keyspace}")
logger.info("Workstate ID: #{workstate_id}")

cluster = Cassandra.connect( hosts:  [host])
session  = cluster.connect(keyspace) 

session.execute(<<CREATETABLE
  CREATE TABLE IF NOT EXISTS workstate (
    id text PRIMARY KEY,
    worker text,
    state text,
    schedule text,
    next text
  )
CREATETABLE
)

# Make sure the workstate row for the specified ID exists
session.execute("INSERT INTO workstate (id) values ('#{workstate_id}') IF NOT EXISTS")


def get_lock(logger,sess,workstate_id,wid)
  stmt = sess.prepare("UPDATE workstate USING TTL 20 SET worker = ? WHERE id = ? IF worker = null")
  begin
    sess.execute(stmt,wid,workstate_id).each do |row|
      return row['[applied]']
    end
  # FIXME: Currently exploring suitable exception handling
  rescue StandardError => e
    logger.info("StandardError occurred, unable to get lock: #{e}")
    return false
  rescue Exception => x
    logger.info("Exception occurred, unable to get lock: #{e}")
    return false
  else
    logger.info("Other Exception occurred, unable to get lock: #{e}")
    return false
  end
end

# TODO Add Exception handling
def free_lock(sess,workstate_id,wid)
  stmt = sess.prepare("UPDATE workstate SET worker = null WHERE id = ? IF worker = ?")
  sess.execute(stmt,workstate_id,wid).each do |row|
    return row['[applied]']
  end
end

# TODO Add Exception handling
def get_state(sess,workstate_id,wid)
  stmt = sess.prepare("select state,worker from workstate WHERE id = ?")
  sess.execute(stmt,workstate_id).each do |row|
    state = row['state']
    worker = row['worker']
    if(worker != wid)
      raise "Worker #{worker} does not match #{wid}"
    end
    return state
  end
end

# TODO Add Exception handling
def set_state(sess,workstate_id,wid,state)
  stmt = sess.prepare("UPDATE workstate SET state = ? WHERE id=?")
  sess.execute(stmt,state,workstate_id)
end


# Do some work that returns a new state *after* the work
def do_work_and_determine_new_state(logger,state)
    logger.info("Starting work with state=#{state}")
    s = state.to_i
    sleep 10
    s += 1
    logger.info("Finished work, neext state is #{s}")
    return s.to_s 
end


loop do
  if(!get_lock(logger,session,workstate_id,worker_id)) 
    #logger.info("Unable to get lock")
    sleep 1
    next
  end

  logger.info("Akquired lock")

  state = get_state(session,workstate_id,worker_id) || 0
  logger.info("Workstate state is #{state}")

  new_state = do_work_and_determine_new_state(logger,state)

  set_state(session,workstate_id,worker_id,new_state)
  if(!free_lock(session,workstate_id,worker_id)) 
    # FIXME: What is the correct way to handle this logical error? 
    puts "Error: unable to free lock"
  end

end






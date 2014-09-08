require 'net/http'
require 'cassandra'
require 'logger'

class Worker

  attr_accessor :host, :workspace_id, :worker_id, :logger, :session

  def initialize(host, keyspace, workspace_id)
    @host = host
    @keyspace = keyspace
    @workspace_id = workspace_id

    @logger = Logger.new(STDERR)
    @logger.level = Logger::INFO

    @logger.info("Started worker #{@worker_id}")
    @logger.info("Host:      #{@host}")
    @logger.info("Keyspace:  #{@keyspace}")
    @logger.info("Workspace: #{@workspace_id}")

    @cluster = Cassandra.connect(hosts: [@host])
    @session = @cluster.connect(keyspace)

    @logger.info("Casandra connection established and got session")

    # For some reason, timeuuid makes problems in the perpared query, use string for now
    @worker_id = Cassandra::TimeUuid::Generator.new.next.to_s


    @session.execute(<<-CREATETABLE
      CREATE TABLE IF NOT EXISTS workspaces (
        workspace_id text PRIMARY KEY,
        worker_id text,
        state text,
        schedule text,
        next text
      )
    CREATETABLE
    )

    # Make sure the workspace row for the specified ID exists
    @session.execute("INSERT INTO workspaces (workspace_id) values ('#{@workspace_id}') IF NOT EXISTS")

    @get_lock_stmt = @session.prepare("UPDATE workspaces USING TTL 20 SET worker_id = ? WHERE workspace_id = ? IF worker_id = null")
    @free_lock_stmt = @session.prepare("UPDATE workspaces SET worker_id = null WHERE workspace_id = ? IF worker_id = ?")
    @get_state_stmt = @session.prepare("SELECT state,worker_id FROM workspaces WHERE workspace_id = ?")
    @set_state_stmt = @session.prepare("UPDATE workspaces SET state = ? WHERE workspace_id = ?")

  end


  def get_lock
    begin
      session.execute(@get_lock_stmt, worker_id, workspace_id).each do |row|
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
  def free_lock
    session.execute(@free_lock_stmt, workspace_id, worker_id).each do |row|
      return row['[applied]']
    end
  end


# TODO Add Exception handling
  def get_state
    session.execute(@get_state_stmt, workspace_id).each do |row|
        state = row['state']
        lock_owning_worker_id = row['worker_id']
        if (lock_owning_worker_id != worker_id)
          raise "Worker #{worker_id} does not match lock holding worker id #{lock_owning_worker_id}"
        end
        return state
    end
    raise "Worker #{worker_id} did not find state for #{workspace_id}"
  end

  def set_state(state)
    # TODO Add Exception handling
    session.execute(@set_state_stmt, state, workspace_id)
  end


end


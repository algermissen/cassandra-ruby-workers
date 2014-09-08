#!/usr/bin/env ruby
#require 'net/http'
require 'net/https'
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



class ConcurrentWorker < Worker

  def initialize(host, keyspace, workspace_id)
   super(host, keyspace, workspace_id)
   @sigterm_received = false
   @start_suri = 'https://api.github.com/repos/datastax/ruby-driver/commits'
  end

  def run

    Signal.trap("INT") {
      trap_sigterm
    }
    Signal.trap("TERM") {
      trap_sigterm
    }

    loop do
      if(!get_lock)
        logger.info("Unable to get lock")
        sleep 1
        next
      end

      logger.info("Akquired lock")

      state = get_state || @start_suri
      logger.info("Workstate state is #{state}")

      uri = URI.parse(state)

      http = Net::HTTP.new(uri.hostname, uri.port)
      http.use_ssl = true
      request = Net::HTTP::Get.new(uri.request_uri)
      http.request(request) do |response|
        if(!response.code != "200")
          logger.info("Server returned #{response.code}, exiting")
          exit
        end
        #Link: <https://api.github.com/repositories/23407534/commits?page=2>; rel="next"
        link_header = response['Link']
        if(!link_header)
          logger.info("No link header found in response; exiting");
          exit
        end
        next_suri = link_header.scan( /<([^>]*)>; rel="next"/).first.first
        if(!next_suri)
          logger.info("No 'next' link found in #{link_header}; exiting");
          exit
        end
        logger.info("Setting next state: #{next_suri}")
        set_state(next_suri)
        if(!free_lock)
          # FIXME: What is the correct way to handle this logical error?
          logger.error("Error: unable to free lock")
        end

        nbytes = 0
        response.read_body do |chunk|
          nbytes += chunk.length
        end

        logger.info("Downloaded #{nbytes} for #{state}")

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


ConcurrentWorker.new(host,keyspace,workspace).run


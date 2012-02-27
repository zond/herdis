
require 'em-synchrony'
require 'em-synchrony/em-http'
require 'hiredis'
require 'redis'
require 'redis/distributed'
require 'goliath'
require 'yajl'
require 'digest/sha1'
require 'pp'

$LOAD_PATH.unshift(File.expand_path('lib'))

require 'herdis/common'

module Herdis
  
  class Client
    
    class DeadClusterException < RuntimeError
    end

    def initialize(url, options = {})
      @options = options
      @shepherds = {"initial" => {"url" => url}}
      begin
        refresh_cluster
      rescue DeadClusterException => e
        raise "No such cluster: #{url}"
      end
    end

    def refresh_cluster
      cluster = nil
      while cluster.nil?
        raise DeadClusterException.new if @shepherds.empty?
        random_shepherd_id = @shepherds.keys[rand(@shepherds.size)]
        cluster_request = 
          EM::HttpRequest.new(@shepherds[random_shepherd_id]["url"]).get(:head => {"Accept" => "application/json"})
        if cluster_request.response_header.status == 0
          @shepherds.delete(random_shepherd_id)
        else
          cluster = Yajl::Parser.parse(cluster_request.response)
        end
      end
      unless cluster["shards"].size == Herdis::Common::SHARDS
        raise "Broken cluster, there should be #{Herdis::Common::SHARDS} shards, but are #{@nodes.size}"
      end
      @shepherds = cluster["shepherds"]
      @dredis = Redis::Distributed.new(cluster["shards"].sort_by do |k,v| k end.collect do |k,v| v end,
                                       @options)
    end      

    def method_missing(meth, *args, &block)
      begin
        @dredis.send(meth, *args, &block)
      rescue DeadClusterException => e
        refresh_cluster
        retry
      end
    end

  end
  
end

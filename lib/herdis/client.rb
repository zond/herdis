
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

    def create_urls(cluster)
      urls = {}
      cluster.each do |shepherd_id, shepherd_status|
        shepherd_url = URI.parse(shepherd_status["url"])
        (shepherd_status["masters"] || []).each do |shard_id|
          urls[shard_id.to_i] = "redis://#{shepherd_url.host}:#{shepherd_status["first_port"].to_i + shard_id.to_i}/"
        end
      end
      urls.keys.sort.collect do |key|
        urls[key]
      end
    end

    def refresh_cluster
      cluster = nil
      while cluster.nil?
        raise DeadClusterException.new if @shepherds.empty?
        random_shepherd_id = @shepherds.keys[rand(@shepherds.size)]
        cluster_request = 
          EM::HttpRequest.new(@shepherds[random_shepherd_id]["url"]).get(:path => "/cluster",
                                                                         :head => {"Accept" => "application/json"})
        if cluster_request.response_header.status == 0
          @shepherds.delete(random_shepherd_id)
        else
          cluster = Yajl::Parser.parse(cluster_request.response)
        end
      end
      urls = create_urls(cluster)
      unless urls.size == Herdis::Common::SHARDS
        raise "Broken cluster, there should be #{Herdis::Common::SHARDS} shards, but are #{urls.size}"
      end
      @shepherds = cluster
      @dredis = Redis::Distributed.new(urls,
                                       @options)
    end      
    
    def method_missing(meth, *args, &block)
      begin
        @dredis.send(meth, *args, &block)
      rescue DeadClusterException => e
        refresh_cluster
        retry
      rescue Errno::ECONNREFUSED => e
        refresh_cluster
        retry
      end
    end

  end
  
end

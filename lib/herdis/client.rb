
require 'hiredis'
require 'redis'
require 'redis/distributed'
require 'yajl'
require 'digest/sha1'
require 'pp'

$LOAD_PATH.unshift(File.expand_path('lib'))

require 'herdis/common'

module Herdis
  
  class Client

    class ReDistributed < Redis::Distributed
      
      attr_reader :nodes

      def initialize(urls, options = {})
        @tag = options.delete(:tag) || /^\{(.+?)\}/
        @default_options = options
        @nodes = urls.map { |url| Redis.connect(options.merge(:url => url)) }
        @subscribed_node = nil
      end

      def node_for(key)
        @nodes[Digest::SHA1.hexdigest(key_tag(key.to_s) || key.to_s).to_i(16) % @nodes.size]
      end
      
      def add_node(url)
        raise "You can't add nodes to #{self}!"
      end
      
    end
    
    class DeadClusterException < RuntimeError
    end

    attr_reader :options, :shepherds, :dredis

    def initialize(*args)
      options = args.last.is_a?(Hash) ? args.pop : {}
      @options = options
      @shepherds = {}
      args.each_with_index do |url, index|
        @shepherds["initial#{index}"] = {"url" => url}
      end
      begin
        refresh_cluster
      rescue DeadClusterException => e
        raise "No such cluster: #{url}"
      end
    end

    def create_urls(cluster)
      hash = {}
      cluster.each do |shepherd_id, shepherd_status|
        shepherd_url = URI.parse(shepherd_status["url"])
        (shepherd_status["masters"] || []).each do |shard_id|
          hash[shard_id.to_i] = "redis://#{shepherd_url.host}:#{shepherd_status["first_port"].to_i + shard_id.to_i}/"
        end
      end
      urls = hash.keys.sort.collect do |key|
        hash[key]
      end
    end

    def validate(urls)
      unless urls.size == Herdis::Common::SHARDS
        raise "Broken cluster, there should be #{Herdis::Common::SHARDS} shards, but are #{urls.size}"
      end
      creators = Set.new
      urls.each_with_index do |url, index|
        parsed = URI.parse(url)
        r = Redis.new(:host => parsed.host, :port => parsed.port)
        claimed_shard = r.get("Herdis::Shepherd::Shard.id").to_i
        creators << r.get("Herdis::Shepherd::Shard.created_by")
        raise "Broken cluster, shard #{index} claims to be shard #{claimed_shard}" unless claimed_shard == index
        raise "Broken cluster, multiple creators: #{creators.inspect}" if creators.size > 1
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
          begin
            urls = create_urls(cluster)
            validate(urls)
          rescue Errno::ECONNREFUSED => e
            cluster = nil
          rescue RuntimeError => e
            if e.message == "ERR operation not permitted"
              cluster = nil
            else
              raise e
            end
          end
        end
      end
      @shepherds = cluster
      @dredis = ReDistributed.new(urls,
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
      rescue RuntimeError => e
        if e.message == "ERR operation not permitted"
          refresh_cluster
          retry
        else
          raise e
        end
      end
    end

  end
  
end

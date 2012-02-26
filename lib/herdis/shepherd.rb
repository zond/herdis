
module Herdis

  class Shepherd

    class Shard
      attr_accessor :port
      attr_accessor :dir
      attr_accessor :redis
      attr_accessor :host
      def initialize(options = {})
        @port = options.delete(:port)
        @dir = options.delete(:dir)
        @redis = options.delete(:redis)
        @host = options.delete(:host) || "127.0.0.1"
        Dir.mkdir(dir) unless Dir.exists?(dir)
        initialize_redis
      end
      def connection
        @connection ||= Redis.new(:host => host, :port => port)
      end
      def to_json
        Yajl::Encoder.encode("redis://#{Fiber.current.host}:#{port}/")
      end
      def inspect
        begin
          super
        rescue Errno::ECONNREFUSED => e
          "#<#{self.class.name} @dir=#{dir} @port=#{port} CLOSED>"
        end
      end
      private
      def initialize_redis
        begin
          connection.ping
        rescue Errno::ECONNREFUSED => e
          io = IO.popen("#{redis} -", "w")
          write_configuration(io)
        end
      end
      def write_configuration(io)
        io.puts("daemonize yes")
        io.puts("pidfile #{dir}/pid")
        io.puts("port #{port}")
        io.puts("timeout 300")
        io.puts("save 900 1")
        io.puts("save 300 10")
        io.puts("save 60 10000")
        io.puts("dbfilename dump.rdb")
        io.puts("dir #{dir}")
        io.puts("logfile stdout")
        io.close
      end
    end

    attr_reader :dir
    attr_reader :redis
    attr_reader :shards
    attr_reader :shepherds
    attr_reader :external_shards
    attr_reader :shepherd_id
    attr_reader :first_port

    def initialize(options = {})
      @dir = options.delete(:dir) || File.join(ENV["HOME"], ".herdis")
      Dir.mkdir(dir) unless Dir.exists?(dir)
      @redis = options.delete(:redis) || "redis-server"
      @first_port = options.delete(:first_port) || 9080
      @shepherds = {}
      @external_shards = {}
      @shepherd_id = options.delete(:shepherd_id) || rand(1 << 256).to_s(36)
      initialize_shards
    end

    def join_cluster(url)
      shutdown
      EM::HttpRequest.new(url).put(:body => Yajl::Encoder.encode(shepherd_status),
                                   :head => {"Content-Type" => "application/json"})
    end

    def accept_shepherd(shepherd_status)
      new_shepherds = shepherds.rmerge(shepherd_status["id"] => shepherd_status)
      new_shepherds.delete(shepherd_id)
      if new_shepherds != shepherds
        @shepherds = new_shepherds
        broadcast_cluster
      end
    end

    def merge_cluster(cluster_status)
      new_shepherds = shepherds.rmerge(cluster_status["shepherds"])
      new_shepherds.delete(shepherd_id)
      new_shards = external_shards.rmerge(cluster_status["shards"])
      if new_shepherds != shepherds || new_shards != external_shards
        @shepherds = new_shepherds
        @external_shards = new_shards
        broadcast_cluster
      end
    end

    def url
      "http://#{Fiber.current.host}:#{Fiber.current.port}"
    end

    def shepherd_status
      {
        "type" => "Shepherd",
        "url" => url,
        "id" => shepherd_id
      }
    end

    def cluster_status
      {
        "type" => "Cluster",
        "shepherds" => shepherds.merge(shepherd_id => shepherd_status),
        "shards" => external_shards.merge(shards)
      }
    end
    
    def shutdown
      shards.each do |shard_id, shard|
        shard.connection.shutdown
      end
      @shards = {}
    end

    def ordinal
      cs = cluster_status
      cs["shepherds"]
    end

    private

    def broadcast_cluster
      multi = EM::Synchrony::Multi.new
      shepherds.each do |shepherd_id, shepherd|
        multi.add(shepherd_id, 
                  EM::HttpRequest.new(shepherd["url"]).aput(:body => Yajl::Encoder.encode(cluster_status),
                                                            :head => {"Content-Type" => "application/json"}))
      end
      multi.perform while !multi.finished?
    end

    def initialize_shards
      @shards = {}
      Herdis::Common::SHARDS.times do |shard_id|
        shards[shard_id] = Shard.new(:redis => redis, 
                                     :dir => File.join(dir, "shard#{shard_id}"), 
                                     :port => first_port + shard_id)
      end
    end


  end

end

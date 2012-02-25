
module Herdis

  class Shepherd

    SHARDS = 128

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
    attr_reader :cluster
    attr_reader :node_id
    attr_reader :first_port

    def initialize(options = {})
      @dir = options.delete(:dir) || File.join(ENV["HOME"], ".herdis")
      Dir.mkdir(dir) unless Dir.exists?(dir)
      @redis = options.delete(:redis) || "redis-server"
      @first_port = options.delete(:first_port) || 9080
      @cluster = {}
      @node_id = options.delete(:node_id) || rand(1 << 256).to_s(36)
      initialize_shards
    end

    def join(url)
      EM::HttpRequest.new(URI.join(url, node_id)).put(:body => Yajl::Encoder.encode(node_status),
                                                      :head => {"Content-Type" => "application/json"})
    end

    def add_node(info)
      merge_cluster(info["id"] => info)
    end

    def node_status
      {
        :url => Fiber.current.public_url,
        :id => node_id,
        :shards => shards.size,
        :live => shards.count do |shard|
          shard.connection.ping == "PONG"
        end
      }
    end

    def cluster_status
      {
        :cluster => cluster.merge(node_id => node_status)
      }
    end
    
    def shutdown
      shards.each do |shard|
        shard.connection.shutdown
      end
    end

    private

    def merge_cluster(info)
      new_cluster = @cluster.merge(info)
      new_cluster.delete(node_id)
      if @cluster != new_cluster
        @cluster = new_cluster
        @cluster.each do |id, node|
          join(node["url"])
        end
      end
    end

    def initialize_shards
      @shards = []
      SHARDS.times do |shard_id|
        shards << Shard.new(:redis => redis, 
                            :dir => File.join(dir, "shard#{shard_id}"), 
                            :port => first_port + shard_id)
      end
    end


  end

end

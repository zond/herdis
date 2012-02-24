
module Herdis

  class Shepherd

    @@instance = nil

    def self.instance
      @@instance ||= Shepherd.new
    end

    SHARDS = 128
    INITIAL_PORT = 9080

    class Sheep
      attr_accessor :port
      attr_accessor :dir
      attr_accessor :redis
      def initialize(redis, dir, port)
        @port = port
        @dir = dir
        @redis = redis
        Dir.mkdir(@dir) unless Dir.exists?(dir)
        initialize_redis
      end
      def connection
        @connection ||= Redis.new(:host => "127.0.0.1", :port => port)
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
    attr_reader :sheep

    def initialize(options = {})
      @dir = options.delete(:dir) || File.join(ENV["HOME"], ".herdis")
      Dir.mkdir(dir) unless Dir.exists?(dir)
      @redis = options.delete(:redis) || "redis-server"
      initialize_sheep
      @@instance = self
    end

    def ping
      sheep.size.to_f / sheep.count do |sheep|
        sheep.connection.ping == "PONG"
      end
    end
    
    def shutdown
      sheep.each do |sheep|
        sheep.connection.shutdown
      end
    end

    private

    def initialize_sheep
      @sheep = []
      SHARDS.times do |shard_id|
        @sheep << Sheep.new(redis, File.join(dir, "shard#{shard_id}"), INITIAL_PORT + shard_id)
      end
    end


  end

end

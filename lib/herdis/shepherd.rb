
module Herdis

  class Shepherd

    class Shard
      attr_accessor :shepherd
      attr_accessor :id
      attr_accessor :slaveof
      def initialize(options = {})
        @shepherd = options.delete(:shepherd)
        @id = options.delete(:id)
        @slaveof = options.delete(:slaveof)
        Dir.mkdir(dir) unless Dir.exists?(dir)
        initialize_redis
      end
      def dir
        File.join(shepherd.dir, "shard#{id}")
      end
      def port
        shepherd.first_port + id
      end
      def inmemory
        shepherd.inmemory
      end
      def connection
        @connection ||= Redis.new(:host => "localhost", :port => port)
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
      def liberate!
        @slaveof = nil
        connection.slaveof("NO", "ONE")
      end
      def initialize_redis
        begin
          connection.ping
        rescue Errno::ECONNREFUSED => e
          io = IO.popen("#{shepherd.redis} -", "w")
          write_configuration(io)
        end
      end
      def write_configuration(io)
        io.puts("daemonize yes")
        io.puts("pidfile #{dir}/pid")
        io.puts("port #{port}")
        io.puts("timeout 300")
        if slaveof
          io.puts("slaveof #{slaveof.host} #{slaveof.port}")
        end
        unless inmemory
          io.puts("save 900 1")
          io.puts("save 300 10")
          io.puts("save 60 10000")
          io.puts("dbfilename dump.rdb")
        end
        io.puts("dir #{dir}")
        io.puts("logfile stdout")
        io.close
      end
    end

    attr_reader :dir
    attr_reader :redis
    attr_reader :shards
    attr_reader :slave_shards
    attr_reader :shepherds
    attr_reader :external_shards
    attr_reader :shepherd_id
    attr_reader :first_port
    attr_reader :inmemory
    attr_reader :check_slave_timer

    def initialize(options = {})
      @dir = options.delete(:dir) || File.join(ENV["HOME"], ".herdis")
      Dir.mkdir(dir) unless Dir.exists?(dir)
      @redis = options.delete(:redis) || "redis-server"
      @first_port = options.delete(:first_port) || 9080
      @inmemory = options.delete(:inmemory)
      @shepherds = {}
      @external_shards = {}
      @shepherd_id = options.delete(:shepherd_id) || rand(1 << 256).to_s(36)
      @slave_shards = {}
      @shards = {}
      Herdis::Common::SHARDS.times do |shard_id|
        create_master_shard(shard_id)
      end
    end

    def join_cluster(url)
      shutdown
      EM::HttpRequest.new(url).put(:body => Yajl::Encoder.encode(shepherd_status),
                                   :head => {"Content-Type" => "application/json"})
    end

    def accept_shepherd(shepherd_status)
      new_shepherds = shepherds.rmerge(shepherd_status["id"] => shepherd_status)
      new_shepherds.delete(shepherd_id)
      update_cluster(new_shepherds, nil)
    end

    def merge_cluster(cluster_status)
      new_shepherds = shepherds.rmerge(cluster_status["shepherds"])
      new_shepherds.delete(shepherd_id)
      new_shards = external_shards.rmerge(cluster_status["shards"])
      update_cluster(new_shepherds, new_shards)
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

    def shutdown_shard(shard_id)
      shards[shard_id].connection.shutdown
      @shards.delete(shard_id)
    end
    
    def shutdown
      shards.keys.each do |shard_id|
        shutdown_shard(shard_id)
      end
    end

    def ordinal
      (shepherds.keys + [shepherd_id]).sort.index(shepherd_id)
    end

    def owned_shards
      rval = Set.new
      index = ordinal
      cluster_size = shepherds.size + 1
      while index < Herdis::Common::SHARDS
        rval << index
        index += cluster_size
      end
      rval
    end

    def update_cluster(new_shepherds, new_external_shards)
      updated = false
      if !new_external_shards.nil? && new_external_shards != external_shards
        updated = true
        @external_shards = new_external_shards
      end
      if !new_shepherds.nil? && new_shepherds != shepherds
        updated = true
        @shepherds = new_shepherds
        update_shards
      end
      broadcast_cluster if updated
    end

    def update_shards
      properly_owned = owned_shards
      running = Set.new(shards.keys)
      (properly_owned - running).each do |shard_id|
        create_slave_shard(shard_id)
      end
    end

    def broadcast_cluster
      multi = EM::Synchrony::Multi.new
      shepherds.each do |shepherd_id, shepherd|
        multi.add(shepherd_id, 
                  EM::HttpRequest.new(shepherd["url"]).aput(:body => Yajl::Encoder.encode(cluster_status),
                                                            :head => {"Content-Type" => "application/json"}))
      end
      multi.perform while !multi.finished?
    end

    def create_master_shard(shard_id)
      shards[shard_id] = create_shard(shard_id)
    end

    def check_slave_shards
      ready_for_liberation = {}
      slave_shards.each do |shard_id, shard|
        ready_for_liberation[shard_id] = shard if shard.connection.info["master_sync_in_progress"] == "0"
      end
    end

    def create_slave_shard(shard_id)
      u = URI.parse(external_shards[shard_id.to_s])
      slave_shards[shard_id] = create_shard(shard_id, :slaveof => u)
      @check_slave_timer ||= EM.add_periodic_timer(10) do
        check_slave_shards
      end
    end

    def create_shard(shard_id, options = {})
      Shard.new(options.merge(:shepherd => self,
                              :id => shard_id,
                              :port => first_port + shard_id))
    end

  end

end

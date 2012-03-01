
module Herdis

  class Shepherd

    CHECK_SLAVE_TIMER = (ENV["SHEPHERD_CHECK_SLAVE_TIMER"] || 10).to_f
    CHECK_PREDECESSOR_TIMER = (ENV["SHEPHERD_CHECK_PREDECESSOR_TIMER"] || 1).to_f

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
        shepherd.first_port + id.to_i
      end
      def inmemory
        shepherd.inmemory
      end
      def connection
        @connection ||= Redis.new(:host => "localhost", :port => port)
      end
      def to_json
        Yajl::Encoder.encode("url" => "redis://#{shepherd.host}:#{port}/",
                             "shepherd_id" => shepherd.shepherd_id)
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
        shepherd.slave_shards.delete(id.to_s)
        shepherd.shards[id.to_s] = self
      end
      def enslave!(url)
        @slaveof = url
        connection.slaveof(url.host, url.port)
        shepherd.shards.delete(id.to_s)
        shepherd.slave_shards[id.to_s] = self
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
    attr_reader :redundancy
    attr_reader :port
    attr_reader :logger

    def initialize(options = {})
      @dir = options.delete(:dir) || File.join(ENV["HOME"], ".herdis")
      Dir.mkdir(dir) unless Dir.exists?(dir)
      @redis = options.delete(:redis) || "redis-server"
      @port = options.delete(:port) || 9000
      @logger = options.delete(:logger)
      @first_port = options.delete(:first_port) || 9080
      @inmemory = options.delete(:inmemory)
      @redundancy = options.delete(:redundancy) || 2
      @shepherds = {}
      @external_shards = {}
      @shepherd_id = options.delete(:shepherd_id) || rand(1 << 256).to_s(36)
      @slave_shards = {}
      @shards = {}
      if connect_to = options.delete(:connect_to)
        join_cluster(connect_to)
      else
        Herdis::Common::SHARDS.times do |shard_id|
          create_master_shard(shard_id)
        end
      end
    end

    def ensure_predecessor_check
      @check_predecessor_timer ||= EM.add_periodic_timer(CHECK_PREDECESSOR_TIMER) do
        check_predecessor
      end
    end

    def join_cluster(url)
      shutdown
      resp = EM::HttpRequest.new(url).put(:body => Yajl::Encoder.encode(shepherd_status),
                                          :head => {"Content-Type" => "application/json"}).response
      merge_cluster(Yajl::Parser.parse(resp))
      ensure_predecessor_check
    end

    def accept_shepherd(shepherd_status)
      new_shepherds = shepherds.rmerge(shepherd_status["id"] => shepherd_status)
      update_cluster(new_shepherds, nil)
      ensure_predecessor_check
    end

    def merge_cluster(cluster_status)
      new_shepherds = shepherds.rmerge(cluster_status["shepherds"].reject do |k,v| k == shepherd_id end)
      new_shards = external_shards.rmerge(cluster_status["shards"])
      (cluster_status["removed_shepherds"] || []).each do |shepherd_id|
        new_shepherds.delete(shepherd_id)
        new_shards.reject! do |key, value|
          value["shepherd_id"] == shepherd_id
        end
      end
      update_cluster(new_shepherds, new_shards)
    end

    def host
      @host ||= Fiber.current.host if Fiber.current.respond_to?(:host)
      @host || "localhost"
    end

    def url
      "http://#{host}:#{port}"
    end

    def shepherd_status
      {
        "type" => "Shepherd",
        "url" => url,
        "id" => shepherd_id
      }
    end

    def cluster_info
      {
        "shepherd_id" => shepherd_id,
        "slaves" => slave_shards.size,
        "masters" => shards.size,
        "redundancy" => redundancy,
        "siblings" => shepherds.keys.sort,
        "inmemory" => inmemory,
        "check_slave_timer" => CHECK_SLAVE_TIMER,
        "check_predecessor_timer" => CHECK_PREDECESSOR_TIMER
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
      shards[shard_id.to_s].connection.shutdown
      @shards.delete(shard_id.to_s)
    end
    
    def shutdown
      shards.keys.each do |shard_id|
        shutdown_shard(shard_id)
      end
      slave_shards.each do |shard_id, shard|
        shard.connection.shutdown
      end
      slave_shards = {}
    end

    def ordinal
      (shepherds.keys + [shepherd_id]).sort.index(shepherd_id)
    end

    def update_cluster(new_shepherds, new_external_shards)
      removed_shepherds = []
      updated = false
      if !new_external_shards.nil? && new_external_shards != external_shards
        updated = true
        @external_shards = new_external_shards
      end
      if !new_shepherds.nil? && new_shepherds != shepherds
        updated = true
        removed_shepherds = (Set.new(@shepherds.keys) - Set.new(new_shepherds.keys)).to_a
        @shepherds = new_shepherds
      end
      if updated
        update_shards
        broadcast_cluster(removed_shepherds)
      end
    end

    def update_shards
      should_be_owned = owned_shards
      should_be_backed_up = backup_shards
      running = Set.new(shards.keys)
      slaves = Set.new(slave_shards.keys)
      externally_running = Set.new(external_shards.reject do |k,v| 
                                   v["shepherd_id"] == shepherd_id 
                                 end.keys)
      #
      # Shards that I ought to own or backup and are running some place else but I don't have a slave or master for should get a slave.
      #
      (((should_be_owned + should_be_backed_up) & externally_running) - (running + slaves)).each do |shard_id|
        create_slave_shard(shard_id)
      end
      #
      # Shards that I have slaves for but nobody else is running should be liberated.
      #
      (slaves - externally_running).each do |shard_id|
        slave_shards[shard_id.to_s].liberate!
      end
      #
      # Shards that both me and someone else are running, and that I shouldn't own or backup, get shutdown.
      #
      ((running & externally_running) - (should_be_owned + should_be_backed_up)).each do |shard_id|
        shutdown_shard(shard_id)
      end
      #
      # Shards that both me and someone else are running, and that I should backup but not own get enslaved.
      #
      ((running & externally_running & should_be_backed_up) - should_be_owned).each do |shard_id|
        shards[shard_id].enslave!(URI.parse(external_shards[shard_id]["url"]))
      end
    end

    def broadcast(message)
      multi = EM::Synchrony::Multi.new
      shepherds.each do |shepherd_id, shepherd|
        multi.add(shepherd_id, 
                  EM::HttpRequest.new(shepherd["url"]).aput(:body => Yajl::Encoder.encode(message),
                                                            :head => {"Content-Type" => "application/json"}))
      end
      multi.perform while !multi.finished?
    end

    def broadcast_cluster(removed_shepherds)
      broadcast(cluster_status.merge("removed_shepherds" => removed_shepherds))
    end

    def create_master_shard(shard_id)
      shards[shard_id.to_s] = create_shard(shard_id)
    end

    def status
      204
    end

    def ordered_shepherd_keys
      (shepherds.keys + [shepherd_id]).sort
    end

    def backup_ordinals
      rval = Set.new
      ordered_keys = ordered_shepherd_keys
      my_index = ordered_keys.index(shepherd_id)
      [redundancy, ordered_keys.size - 1].min.times do |n|
        rval << (my_index - n - 1) % ordered_keys.size
      end
      rval
    end

    def backup_shards
      rval = Set.new
      backup_ordinals.each do |ordinal|
        rval += shards_owned_by(ordinal)
      end
      rval
    end

    def shards_owned_by(ordinal)
      rval = Set.new
      cluster_size = shepherds.size + 1
      while ordinal < Herdis::Common::SHARDS
        rval << ordinal.to_s
        ordinal += cluster_size
      end
      rval
    end

    def owned_shards
      shards_owned_by(ordinal)
    end

    def predecessor
      ordered_keys = ordered_shepherd_keys
      my_index = ordered_keys.index(shepherd_id)
      if my_index == 0
        shepherds[ordered_keys.last]
      else
        shepherds[ordered_keys[my_index - 1]]
      end
    end

    def check_predecessor
      pre = predecessor
      if pre
        Fiber.new do
          if EM::HttpRequest.new(pre["url"]).head.response_header.status != 204
            merge_cluster(Yajl::Parser.parse(Yajl::Encoder.encode(cluster_status)).merge("removed_shepherds" => [pre["id"]]))
          end
        end.resume
      end
    end

    #
    # Check what shards we should own that we have synced slaves for.
    #
    def check_slave_shards
      ready_for_liberation = {}
      (owned_shards & Set.new(slave_shards.keys)).each do |shard_id|
        shard = slave_shards[shard_id.to_s]
        if shard.connection.info["master_sync_in_progress"] == "0"
          current_owner = external_shards[shard_id.to_s]["shepherd_id"]
          ready_for_liberation[current_owner] ||= {}
          ready_for_liberation[current_owner][shard_id] = shard 
        end
      end
      ready_for_liberation.each do |shepherd_id, shards|
        #
        # Create a message where we take over the slave shards owned by this shepherd.
        #
        body = {
          "type" => "Cluster",
          "shepherds" => {self.shepherd_id => self.shepherd_status},
          "shards" => shards
        }
        Fiber.new do
          if shepherds[shepherd_id]
            EM::HttpRequest.new(shepherds[shepherd_id]["url"]).put(:body => Yajl::Encoder.encode(body),
                                                                   :head => {"Content-Type" => "application/json"})
          else
            broadcast(body)
          end
        end.resume
      end
    end

    def create_slave_shard(shard_id)
      u = URI.parse(external_shards[shard_id.to_s]["url"])
      slave_shards[shard_id.to_s] = create_shard(shard_id, :slaveof => u)
      @check_slave_timer ||= EM.add_periodic_timer(CHECK_SLAVE_TIMER) do
        check_slave_shards
      end
    end

    def create_shard(shard_id, options = {})
      Shard.new(options.merge(:shepherd => self,
                              :id => shard_id))
    end

  end

end

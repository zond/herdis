
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
        unless url == slaveof
          @slaveof = url
          connection.slaveof(url.host, url.port)
          shepherd.shards.delete(id.to_s)
          shepherd.slave_shards[id.to_s] = self
        end
      end
      def initialize_redis
        begin
          connection.ping
          if slaveof
            connection.slaveof(slaveof.host, slaveof.port)
          else
            connection.slaveof("NO", "ONE")
          end
        rescue Errno::ECONNREFUSED => e
          io = IO.popen("#{shepherd.redis} -", "w")
          write_configuration(io)
        end
        initialization = Proc.new do
          begin
            connection.set("#{self.class.name}.id", id)
            connection.set("#{self.class.name}.created_at", Time.now.to_i)
            connection.set("#{self.class.name}.created_by", shepherd.shepherd_id)
          rescue Errno::ECONNREFUSED => e
            EM.add_timer(0.1) do
              self.call
            end
          end
        end
        EM.add_timer(0.1) do
          initialization.call
        end
      end
      def write_configuration(io)
        io.puts("daemonize yes")
        io.puts("pidfile #{dir}/pid")
        io.puts("port #{port}")
        io.puts("timeout 300")
        if slaveof
          @slaveof = slaveof
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

    def ensure_slave_check
      @check_slave_timer ||= EM.add_periodic_timer(CHECK_SLAVE_TIMER) do
        check_slave_shards
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
    end

    def accept_shepherd(shepherd_status)
      new_shepherds = shepherds.rmerge(shepherd_status["id"] => shepherd_status)
      update_cluster(new_shepherds, nil)
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
        "ordinal" => ordinal,
        "slaves" => slave_shards.size,
        "masters" => shards.size,
        "redundancy" => redundancy,
        "siblings" => shepherds.keys.sort,
        "inmemory" => inmemory,
        "check_slave_timer" => CHECK_SLAVE_TIMER,
        "check_predecessor_timer" => CHECK_PREDECESSOR_TIMER,
        "shards" => "#{url}/shards",
        "sanity" => "#{url}/sanity"
      }
    end

    def sanity
      creators = Set.new
      min_created_at = nil
      max_created_at = nil
      masters = 0
      slaves = 0
      consistent = true
      shards.each do |shard_id, shard|
        info = shard.connection.info
        masters += 1 if info["role"] == "master"
        slaves += 1 if info["role"] == "slave"
        created_at = shard.connection.get("#{Herdis::Shepherd::Shard.name}.created_at").to_i
        min_created_at = created_at if min_created_at.nil? || created_at < min_created_at
        max_created_at = created_at if max_created_at.nil? || created_at > max_created_at
        creators << shard.connection.get("#{Herdis::Shepherd::Shard.name}.created_by")
        consistent &= (shard.connection.get("#{Herdis::Shepherd::Shard.name}.id") == shard_id.to_s)
      end
      external_shards.each do |shard_id, shard_data|
        if shard_data["shepherd_id"] != shepherd_id
          url = URI.parse(shard_data["url"])
          shard_connection = Redis.new(:host => url.host, :port => url.port)
          info = shard_connection.info
          masters += 1 if info["role"] == "master"
          slaves += 1 if info["role"] == "slave"
          created_at = shard_connection.get("#{Herdis::Shepherd::Shard.name}.created_at").to_i
          min_created_at = created_at if min_created_at.nil? || created_at < min_created_at
          max_created_at = created_at if max_created_at.nil? || created_at > max_created_at
          creators << shard_connection.get("#{Herdis::Shepherd::Shard.name}.created_by")
          consistent &= (shard_connection.get("#{Herdis::Shepherd::Shard.name}.id") == shard_id.to_s)
        end
      end
      {
        :creators => creators.to_a,
        :consistent => creators.size == 1 && masters == 128 && slaves == 0,
        :min_created_at => Time.at(min_created_at).to_s,
        :max_created_at => Time.at(max_created_at).to_s,
        :masters => masters,
        :slaves => slaves
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

    def shutdown_slave(shard_id)
      slave_shards[shard_id.to_s].connection.shutdown
      @slave_shards.delete(shard_id.to_s)
    end
    
    def shutdown
      shards.keys.each do |shard_id|
        shutdown_shard(shard_id)
      end
      slave_shards.keys.each do |shard_id|
        shutdown_slave(shard_id)
      end
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
      masters = Set.new(shards.keys)
      slaves = Set.new(slave_shards.keys)
      externally_running = Set.new(external_shards.reject do |k,v| 
                                   v["shepherd_id"] == shepherd_id 
                                 end.keys)

      needs_to_be_liberated = slaves - externally_running
      needs_to_be_enslaved = (masters & externally_running & should_be_backed_up) - should_be_owned
      needs_to_be_directed = slaves & externally_running & (should_be_backed_up | should_be_owned)
      slaves_needing_to_be_shut_down = (slaves & externally_running) - (should_be_backed_up | should_be_owned)
      masters_needing_to_be_shut_down = (masters & externally_running) - (should_be_backed_up | should_be_owned)
      new_slaves_needed = ((should_be_backed_up | should_be_owned) & externally_running) - (slaves | masters)

      handled = Set.new

      logger.info "#{shepherd_id} liberating #{needs_to_be_liberated.inspect}" unless needs_to_be_liberated.empty?
      needs_to_be_liberated.each do |shard_id|
        handled.add(shard_id.to_s)
        slave_shards[shard_id.to_s].liberate!
      end
      logger.info "#{shepherd_id} enslaving #{needs_to_be_enslaved.inspect}" unless needs_to_be_enslaved.empty?
      needs_to_be_enslaved.each do |shard_id|
        raise "Already liberated #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        shards[shard_id.to_s].enslave!(URI.parse(external_shards[shard_id.to_s]["url"]))
      end
      needs_to_be_directed.each do |shard_id|
        raise "Already liberated or enslaved #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        slave_shards[shard_id.to_s].enslave!(URI.parse(external_shards[shard_id.to_s]["url"]))
      end
      logger.info "#{shepherd_id} killing masters #{masters_needing_to_be_shut_down.inspect}" unless masters_needing_to_be_shut_down.empty?
      masters_needing_to_be_shut_down.each do |shard_id|
        raise "Already liberated, enslaved or directed #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        shutdown_shard(shard_id)
      end
      logger.info "#{shepherd_id} killing slaves #{slaves_needing_to_be_shut_down.inspect}" unless slaves_needing_to_be_shut_down.empty?
      slaves_needing_to_be_shut_down.each do |shard_id|
        raise "Already liberated, enslaved, directed or shut down #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        shutdown_slave(shard_id)
      end
      logger.info "#{shepherd_id} creating slaves #{new_slaves_needed.inspect}" unless new_slaves_needed.empty?
      new_slaves_needed.each do |shard_id|
        raise "Already liberated, enslaved, directed or shut down #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        create_slave_shard(shard_id)
      end
      ensure_slave_check
      ensure_predecessor_check
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
      slave_owners = Set.new
      to_liberate = {}
      (owned_shards & Set.new(slave_shards.keys)).each do |shard_id|
        shard = slave_shards[shard_id.to_s]
         if shard.connection.info["master_sync_in_progress"] == "0"
          slave_owners << external_shards[shard_id.to_s]["shepherd_id"]
          to_liberate[shard_id] = shard 
        end
      end
      unless to_liberate.empty?
        body = {
          "type" => "Cluster",
          "shepherds" => {self.shepherd_id => self.shepherd_status},
          "shards" => to_liberate
        }
        Fiber.new do
          broadcast(body)
        end.resume
      end
    end

    def create_slave_shard(shard_id)
      u = URI.parse(external_shards[shard_id.to_s]["url"])
      slave_shards[shard_id.to_s] = create_shard(shard_id, :slaveof => u)
    end

    def create_shard(shard_id, options = {})
      Shard.new(options.merge(:shepherd => self,
                              :id => shard_id))
    end

  end

end

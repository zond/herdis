
module Herdis

  class Shepherd

    CHECK_SLAVE_TIMER = (ENV["SHEPHERD_CHECK_SLAVE_TIMER"] || 10).to_f
    CHECK_PREDECESSOR_TIMER = (ENV["SHEPHERD_CHECK_PREDECESSOR_TIMER"] || 1).to_f

    class Shard

      attr_reader :shepherd
      attr_reader :id
      attr_reader :master
      def initialize(options = {})
        @shepherd = options.delete(:shepherd)
        @id = options.delete(:id)
        @master = options.delete(:master)
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
      def inspect
        begin
          super
        rescue Errno::ECONNREFUSED => e
          "#<#{self.class.name} @dir=#{dir} @port=#{port} CLOSED>"
        end
      end
      def liberate!
        @master = nil
        begin
          connection.slaveof("NO", "ONE")
          shepherd.slaves.delete(id.to_s)
          shepherd.masters[id.to_s] = self
        rescue RuntimeError => e
          if e.message == "LOADING Redis is loading the dataset in memory"
            EM::Synchrony.sleep(0.1)
          else
            raise e
          end
        end
      end
      def enslave!(external_uri)
        unless external_uri == master
          @master = external_uri
          connection.slaveof(master.host, master.port)
          shepherd.masters.delete(id.to_s)
          shepherd.slaves[id.to_s] = self
        end
      end
      def initialize_redis
        begin
          connection.ping
          if master
            connection.slaveof(master.host, master.port)
          else
            connection.slaveof("NO", "ONE")
          end
        rescue Errno::ECONNREFUSED => e
          io = IO.popen("#{shepherd.redis} -", "w")
          write_configuration(io)
        end
        unless master
          initialization = Proc.new do |p|
            begin
              connection.set("#{self.class.name}.id", id)
              connection.set("#{self.class.name}.created_at", Time.now.to_i)
              connection.set("#{self.class.name}.created_by", shepherd.shepherd_id)
            rescue Errno::ECONNREFUSED => e
              EM.add_timer(0.1) do
                p.call(p)
              end
            end
          end
          EM.add_timer(0.1) do
            initialization.call(initialization)
          end
        end
      end
      def write_configuration(io)
        io.puts("daemonize yes")
        io.puts("pidfile #{dir}/pid")
        io.puts("port #{port}")
        io.puts("timeout 300")
        if master
          io.puts("slaveof #{master.host} #{master.port}")
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
    attr_reader :first_port
    attr_reader :inmemory
    attr_reader :shepherd_id
    attr_reader :redundancy
    attr_reader :port
    attr_reader :logger

    attr_reader :masters
    attr_reader :slaves
    attr_reader :shepherds

    def initialize(options = {})
      @dir = options.delete(:dir) || File.join(ENV["HOME"], ".herdis")
      @redis = options.delete(:redis) || "redis-server"
      @port = options.delete(:port) || 9000
      @logger = options.delete(:logger)
      @first_port = options.delete(:first_port) || 9080
      @inmemory = options.delete(:inmemory)
      @redundancy = options.delete(:redundancy) || 2
      @shepherd_id = options.delete(:shepherd_id) || rand(1 << 256).to_s(36)
      Dir.mkdir(dir) unless Dir.exists?(dir)

      @shepherds = {}
      @slaves = {}
      @masters = {}

      if connect_to = options.delete(:connect_to)
        join_cluster(connect_to)
      else
        Herdis::Common::SHARDS.times do |shard_id|
          create_master_shard(shard_id)
        end
        @shepherds[shepherd_id] = shepherd_status
      end
    end

    def ensure_slave_check
      @check_slave_timer ||= EM.add_periodic_timer(CHECK_SLAVE_TIMER) do
        check_slaves
      end
    end

    def ensure_predecessor_check
      @check_predecessor_timer ||= EM.add_periodic_timer(CHECK_PREDECESSOR_TIMER) do
        check_predecessor
      end
    end

    def to_each_sibling(method, options, &block)
      default_options = {:head => {"Content-Type" => "application/json"}}
      multi = EM::Synchrony::Multi.new
      shepherds.each do |shepherd_id, shepherd|
        multi.add(shepherd_id, 
                  EM::HttpRequest.new(shepherd["url"]).send(method, 
                                                            default_options.rmerge(options)))
      end
      yield
      Fiber.new do
        multi.perform while !multi.finished?
      end.resume
    end

    def join_cluster(url)
      shutdown
      @shepherds = Yajl::Parser.parse(EM::HttpRequest.new(url).get(:path => "/cluster",
                                                                   :head => {"Content-Type" => "application/json"}).response)
      add_shepherd(shepherd_status)
    end

    def add_shepherd(shepherd_status)
      unless shepherd_status == shepherds[shepherd_status["id"]]
        shepherds[shepherd_status["id"]] = shepherd_status
        to_each_sibling(:aput,
                        :path => "/#{shepherd_status["id"]}",
                        :body => Yajl::Encoder.encode(shepherd_status)) do
          check_shards
        end
      end
    end

    def remove_shepherd(shepherd_id)
      if shepherds.include?(shepherd_id)
        shepherds.delete(shepherd_id)
        to_each_sibling(:adelete,
                        :path => "/#{shepherd_id}") do
          check_shards
        end
      end
    end

    def add_shard(shepherd_id, shard_id)
      unless shepherds[shepherd_id]["masters"].include?(shard_id)
        puts "adding #{shepherd_id}/#{shard_id}"
        shepherds[shepherd_id]["masters"] << shard_id 
        to_each_sibling(:aput,
                        :path => "/#{shepherd_id}/#{shard_id}") do
          check_shards
        end
      end
    end

    def remove_shard(shepherd_id, shard_id)
      if shepherds[shepherd_id]["masters"].include?(shard_id)
        puts "removing #{shepherd_id}/#{shard_id}"
        shepherds[shepherd_id]["masters"].delete(shard_id)
        to_each_sibling(:adelete,
                        :path => "/#{shepherd_id}/#{shard_id}") do
          check_shards
        end
      end
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
        "url" => url,
        "id" => shepherd_id,
        "first_port" => first_port,
        "masters" => masters.keys
      }
    end

    def cluster_info
      {
        "shepherd_id" => shepherd_id,
        "ordinal" => ordinal,
        "slaves" => slaves.keys,
        "masters" => masters.keys,
        "redundancy" => redundancy,
        "siblings" => shepherds.keys.sort,
        "inmemory" => inmemory,
        "check_slave_timer" => CHECK_SLAVE_TIMER,
        "check_predecessor_timer" => CHECK_PREDECESSOR_TIMER,
        "shards" => "#{url}/shards",
        "sanity" => "#{url}/sanity",
        "cluster" => "#{url}/cluster"
      }
    end

    def sanity
      creators = Set.new
      min_created_at = nil
      max_created_at = nil
      masters = 0
      slaves = 0
      consistent = true
      shard_status.each do |shard_url|
        url = URI.parse(shard_url)
        shard_connection = Redis.new(:host => url.host, :port => url.port)
        info = shard_connection.info
        masters += 1 if info["role"] == "master"
        slaves += 1 if info["role"] == "slave"
        created_at = shard_connection.get("#{Herdis::Shepherd::Shard.name}.created_at").to_i
        min_created_at = created_at if min_created_at.nil? || created_at < min_created_at
        max_created_at = created_at if max_created_at.nil? || created_at > max_created_at
        creators << shard_connection.get("#{Herdis::Shepherd::Shard.name}.created_by")
      end
      {
        :creators => creators.to_a,
        :consistent => creators.size == 1 && masters == Herdis::Common::SHARDS && slaves == 0,
        :min_created_at => Time.at(min_created_at).to_s,
        :max_created_at => Time.at(max_created_at).to_s,
        :masters => masters,
        :slaves => slaves
      }
    end

    def shard_status
      rval = []
      cluster_status.each do |shepherd_id, shepherd_status|
        shepherd_status["masters"].each do |shard_id|
          if rval[shard_id.to_i].nil?
            rval[shard_id.to_i] = "redis://#{host}:#{shard_id.to_i + shepherd_status["first_port"].to_i}/"
          else
            raise Goliath::Validations::InternalServerError.new("Duplicate masters: #{shard_id}")
          end
        end
      end
      rval
    end

    def cluster_status
      shepherds.merge(shepherd_id => shepherd_status)
    end

    def shutdown_shard(shard_id)
      if shard = masters[shard_id.to_s]
        shard.connection.shutdown
        masters.delete(shard_id.to_s)
      end
    end

    def shutdown_slave(shard_id)
      if shard = slaves[shard_id.to_s]
        shard.connection.shutdown
        slaves.delete(shard_id.to_s)
      end
    end

    def shutdown
      masters.keys.each do |shard_id|
        shutdown_shard(shard_id)
      end
      slaves.keys.each do |shard_id|
        shutdown_slave(shard_id)
      end
    end

    def ordinal
      (shepherds.keys + [shepherd_id]).sort.index(shepherd_id)
    end

    def create_external_shards(should_be_owned)
      shepherds.values.inject({}) do |sum, shepherd_status|
        if shepherd_status["id"] == shepherd_id
          sum
        else
          shepherd_url = URI.parse(shepherd_status["url"])
          sum.merge(shepherd_status["masters"].inject({}) do |sum, shard_id|
                      shepherd_url = URI.parse(shepherd_status["url"])
                      sum.merge(shard_id.to_s => URI.parse("redis://#{shepherd_url.host}:#{shepherd_status["first_port"].to_i + shard_id.to_i}/"))
                    end)
        end    
      end
    end

    def check_shards
      should_be_owned = owned_shards
      should_be_backed_up = backup_shards
      master_ids = Set.new(masters.keys)
      slave_ids = Set.new(slaves.keys)
      external_shards = create_external_shards(should_be_owned)
      externally_running = Set.new(external_shards.keys)

      needs_to_be_liberated = slave_ids - externally_running
      needs_to_be_enslaved = (master_ids & externally_running & should_be_backed_up) - should_be_owned
      needs_to_be_directed = slave_ids & externally_running & (should_be_backed_up | should_be_owned)
      slaves_needing_to_be_shut_down = (slave_ids & externally_running) - (should_be_backed_up | should_be_owned)
      masters_needing_to_be_shut_down = (master_ids & externally_running) - (should_be_backed_up | should_be_owned)
      new_slaves_needed = ((should_be_backed_up | should_be_owned) & externally_running) - (slave_ids | master_ids)

      handled = Set.new

      logger.info "#{shepherd_id} *** liberating #{needs_to_be_liberated.inspect}" unless needs_to_be_liberated.empty?
      needs_to_be_liberated.each do |shard_id|
        handled.add(shard_id.to_s)
        slaves[shard_id.to_s].liberate!
        add_shard(shepherd_id, shard_id.to_s)
      end
      logger.info "#{shepherd_id} *** enslaving #{needs_to_be_enslaved.inspect}" unless needs_to_be_enslaved.empty?
      needs_to_be_enslaved.each do |shard_id|
        raise "Already liberated #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        masters[shard_id.to_s].enslave!(external_shards[shard_id.to_s])
        remove_shard(shepherd_id, shard_id.to_s)
      end
      needs_to_be_directed.each do |shard_id|
        raise "Already liberated or enslaved #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        slaves[shard_id.to_s].enslave!(external_shards[shard_id.to_s])
      end
      logger.info "#{shepherd_id} *** killing masters #{masters_needing_to_be_shut_down.inspect}" unless masters_needing_to_be_shut_down.empty?
      masters_needing_to_be_shut_down.each do |shard_id|
        raise "Already liberated, enslaved or directed #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        shutdown_shard(shard_id)
        remove_shard(shepherd_id, shard_id.to_s)
      end
      logger.info "#{shepherd_id} *** killing slaves #{slaves_needing_to_be_shut_down.inspect}" unless slaves_needing_to_be_shut_down.empty?
      slaves_needing_to_be_shut_down.each do |shard_id|
        raise "Already liberated, enslaved, directed or shut down #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        shutdown_slave(shard_id)
      end
      logger.info "#{shepherd_id} *** creating slaves #{new_slaves_needed.inspect}" unless new_slaves_needed.empty?
      new_slaves_needed.each do |shard_id|
        raise "Already liberated, enslaved, directed or shut down #{shard_id}!" if handled.include?(shard_id.to_s)
        handled.add(shard_id.to_s)
        create_slave_shard(shard_id.to_s, external_shards[shard_id.to_s])
      end
      
      ensure_slave_check
      ensure_predecessor_check
    end
    
    def check_slaves
      (owned_shards & slaves.keys).each do |shard_id|
        shard = slaves[shard_id.to_s]
        if shard.connection.info["master_sync_in_progress"] == "0"
          updated = true
          shard.liberate!
          add_shard(shepherd_id, shard_id.to_s)
        end
      end
    end

    def create_master_shard(shard_id)
      masters[shard_id.to_s] = create_shard(shard_id)
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
      while ordinal < Herdis::Common::SHARDS
        rval << ordinal.to_s
        ordinal += shepherds.size
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
            remove_shepherd(pre["id"])
          end
        end.resume
      end
    end

    def create_slave_shard(shard_id, external_uri)
      slaves[shard_id.to_s] = create_shard(shard_id, :master => external_uri)
    end

    def create_shard(shard_id, options = {})
      Shard.new(options.merge(:shepherd => self,
                              :id => shard_id))
    end

  end

end

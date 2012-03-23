module Herdis
  module Plugins
    class ShepherdConnection

      @@shepherd = nil
      
      def self.shepherd
        @@shepherd
      end
      
      def self.shutdown
        @@shepherd.shutdown unless @@shepherd.nil?
        @@shepherd = nil
      end
      
      def self.shutdown_cluster
        @@shepherd.shutdown_cluster unless @@shepherd.nil?
        @@shepherd = nil
      end
      
      def initialize(port, config, status, logger)
        @port = port
        @logger = logger
      end

      def run
        opts = {}
        copy_from_env(opts, :first_port, :to_i)
        copy_from_env(opts, :dir)
        copy_from_env(opts, :host)
        copy_from_env(opts, :restart)
        copy_from_env(opts, :port, :to_i)
        copy_from_env(opts, :shepherd_id)
        copy_from_env(opts, :inmemory)
        copy_from_env(opts, :redundancy, :to_i)
        copy_from_env(opts, :connect_to)
        opts[:port] = @port
        opts[:logger] = @logger
        @@shepherd = Herdis::Shepherd.new(opts)
      end
      
      private
      
      def copy_from_env(hash, key, *mutators)
        env_key = key.to_s.upcase
        env_key = "SHEPHERD_#{env_key}" unless env_key.index("SHEPHERD_") == 0
        if ENV[env_key]
          hash[key] = ENV[env_key] 
          mutators.each do |mutator|
            hash[key] = hash[key].send(mutator)
          end
        end
      end

    end
  end
end


require 'em-synchrony'
require 'em-synchrony/em-http'
require 'hiredis'
require 'redis'
require 'goliath'
require 'yajl'
require 'pp'

$LOAD_PATH.unshift(File.expand_path('lib'))

require 'herdis/rmerge'
require 'herdis/common'
require 'herdis/shepherd'
require 'herdis/rack/default_headers'
require 'herdis/rack/shepherd_notifier'
require 'herdis/rack/favicon'
require 'herdis/handlers/common'
require 'herdis/handlers/index'
require 'herdis/handlers/join_cluster'
require 'herdis/handlers/update_cluster'

module Herdis

  class Server < Goliath::API

    use Herdis::Rack::Favicon, File.join(File.dirname(__FILE__), "..", "..", "assets", "shepherd.png")

    @@shepherd = nil

    def self.shepherd
      @@shepherd
    end

    def initialize
      opts = {}
      opts[:first_port] = ENV["SHEPHERD_FIRST_PORT"].to_i if ENV["SHEPHERD_FIRST_PORT"]
      opts[:dir] = ENV["SHEPHERD_DIR"] if ENV["SHEPHERD_DIR"]
      opts[:node_id] = ENV["SHEPHERD_NODE_ID"] if ENV["SHEPHERD_NODE_ID"]
      @@shepherd = Herdis::Shepherd.new(opts)
    end
    
    get '/', Herdis::Handlers::Index
    post '/', Herdis::Handlers::JoinCluster
    put '/', Herdis::Handlers::UpdateCluster
    

  end

end

Goliath::Application.app_class = Herdis::Server


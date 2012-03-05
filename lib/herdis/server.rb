
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
require 'herdis/plugins/shepherd_connection'
require 'herdis/rack/default_headers'
require 'herdis/rack/favicon'
require 'herdis/rack/host_parameter'
require 'herdis/handlers/common'
require 'herdis/handlers/shards'
require 'herdis/handlers/cluster'
require 'herdis/handlers/join_cluster'
require 'herdis/handlers/remove_shepherd'
require 'herdis/handlers/shutdown'
require 'herdis/handlers/ping'
require 'herdis/handlers/info'
require 'herdis/handlers/sanity'
require 'herdis/handlers/add_shepherd'
require 'herdis/handlers/add_shard'
require 'herdis/handlers/remove_shepherd'
require 'herdis/handlers/remove_shard'

module Herdis

  class Server < Goliath::API

    plugin Herdis::Plugins::ShepherdConnection
    use Herdis::Rack::Favicon, File.join(File.dirname(__FILE__), "..", "..", "assets", "shepherd.png")
    
    head '/', Herdis::Handlers::Ping
    get '/', Herdis::Handlers::Info
    get '/shards', Herdis::Handlers::Shards
    get '/cluster', Herdis::Handlers::Cluster
    get '/sanity', Herdis::Handlers::Sanity
    post '/', Herdis::Handlers::JoinCluster
    put '/:shepherd_id/:shard_id', Herdis::Handlers::AddShard
    put '/:shepherd_id', Herdis::Handlers::AddShepherd
    delete '/', Herdis::Handlers::Shutdown
    delete '/:shepherd_id/:shard_id', Herdis::Handlers::RemoveShard
    delete '/:shepherd_id', Herdis::Handlers::RemoveShepherd

  end

end

Goliath::Application.app_class = Herdis::Server


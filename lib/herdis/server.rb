
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
require 'herdis/handlers/index'
require 'herdis/handlers/join_cluster'
require 'herdis/handlers/update_cluster'
require 'herdis/handlers/shutdown'
require 'herdis/handlers/ping'
require 'herdis/handlers/info'

module Herdis

  class Server < Goliath::API

    plugin Herdis::Plugins::ShepherdConnection
    use Herdis::Rack::Favicon, File.join(File.dirname(__FILE__), "..", "..", "assets", "shepherd.png")
    
    get '/', Herdis::Handlers::Index
    get '/info', Herdis::Handlers::Info
    head '/', Herdis::Handlers::Ping
    post '/', Herdis::Handlers::JoinCluster
    put '/', Herdis::Handlers::UpdateCluster
    delete '/', Herdis::Handlers::Shutdown

  end

end

Goliath::Application.app_class = Herdis::Server


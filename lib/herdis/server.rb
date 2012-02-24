
require 'herdis/rack/default_headers'
require 'herdis/handlers/common'
require 'herdis/handlers/index'

module Herdis

  class Server < Goliath::API
    
    get '/', Herdis::Handlers::Index

  end

end

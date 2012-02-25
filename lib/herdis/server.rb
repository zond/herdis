
require 'herdis/rack/default_headers'
require 'herdis/rack/shepherd_notifier'
require 'herdis/handlers/common'
require 'herdis/handlers/index'

module Herdis

  class Server < Goliath::API

    @@shepherd = nil

    def self.shepherd
      @@shepherd
    end

    def initialize
      @@shepherd = Herdis::Shepherd.new
    end
    
    get '/', Herdis::Handlers::Index

  end

end

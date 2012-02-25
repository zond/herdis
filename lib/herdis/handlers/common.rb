
module Herdis
  module Handlers
    module Common
      
      def self.included(base)
        base.use AsyncRack::Runtime
        base.use AsyncRack::Deflater
        base.use Goliath::Rack::DefaultMimeType
        base.use Goliath::Rack::Formatters::JSON
        base.use Herdis::Rack::DefaultHeaders
        base.use Herdis::Rack::ShepherdNotifier
      end
      
    end
  end
end

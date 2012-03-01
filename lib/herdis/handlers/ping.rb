
module Herdis

   module Handlers

    class Ping < Goliath::API
      include Common

      def response(env)
        [Herdis::Plugins::ShepherdConnection.shepherd.status, {}, ""]
      end
      
    end

  end

end

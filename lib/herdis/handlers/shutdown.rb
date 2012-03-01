
module Herdis

   module Handlers

    class Shutdown < Goliath::API
      include Common

      def response(env)
        Herdis::Plugins::ShepherdConnection.shutdown
        [200, {}, ""]
      end
      
    end

  end

end

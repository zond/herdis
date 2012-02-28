
module Herdis

   module Handlers

    class Shutdown < Goliath::API
      include Common

      def response(env)
        Server.shutdown
        [200, {}, ""]
      end
      
    end

  end

end

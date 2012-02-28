
module Herdis

   module Handlers

    class Ping < Goliath::API
      include Common

      def response(env)
        [Server.shepherd.status, {}, ""]
      end
      
    end

  end

end

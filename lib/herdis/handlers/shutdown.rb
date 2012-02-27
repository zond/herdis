
module Herdis

   module Handlers

    class Shutdown < Goliath::API
      include Common

      def response(env)
        Server.shepherd.shutdown
        [200, {}, Server.shepherd.cluster_status]
      end
      
    end

  end

end

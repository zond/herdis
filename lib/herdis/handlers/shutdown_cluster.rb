
module Herdis

   module Handlers

    class ShutdownCluster < Goliath::API
      include Common

      def response(env)
        Herdis::Plugins::ShepherdConnection.shutdown_cluster
        [200, {}, ""]
      end
      
    end

  end

end

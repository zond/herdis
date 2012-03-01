
module Herdis

   module Handlers

    class JoinCluster < Goliath::API
      include Common

      def response(env)
        Herdis::Plugins::ShepherdConnection.shepherd.join_cluster(env['params']['url'])
        [200, {}, Herdis::Plugins::ShepherdConnection.shepherd.cluster_status]
      end
      
    end

  end

end

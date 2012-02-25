
module Herdis

   module Handlers

    class JoinCluster < Goliath::API
      include Common

      def response(env)
        Server.shepherd.join_cluster(env['params']['url'])
        [200, {}, Server.shepherd.cluster_status]
      end
      
    end

  end

end

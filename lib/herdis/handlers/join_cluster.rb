
module Herdis

   module Handlers

    class JoinCluster < Goliath::API
      include Common

      def response(env)
        Herdis::Plugins::ShepherdConnection.shepherd.join_cluster(env['params']['url'])
        [204, {}, ""]
      end
      
    end

  end

end

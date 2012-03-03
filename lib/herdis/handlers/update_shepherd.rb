
module Herdis

   module Handlers

    class UpdateShepherd < Goliath::API
      include Common

      def response(env)
        Herdis::Plugins::ShepherdConnection.shepherd.update_shepherd(env['params'])
        [200, {}, Herdis::Plugins::ShepherdConnection.shepherd.cluster_status]
      end
      
    end

  end

end

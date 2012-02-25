
module Herdis

   module Handlers

    class AddNode < Goliath::API
      include Common

      def response(env)
        Server.shepherd.add_node(env['params'])
        [200, {}, Server.shepherd.cluster_status]
      end
      
    end

  end

end

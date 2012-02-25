
module Herdis

   module Handlers

    class Join < Goliath::API
      include Common

      def response(env)
        Server.shepherd.join(env['params']['url'])
        [200, {}, Server.shepherd.cluster_status]
      end
      
    end

  end

end

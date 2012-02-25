
module Herdis

   module Handlers

    class Index < Goliath::API
      include Common

      def response(env)
        [200, {}, Server.shepherd.cluster_status]
      end
      
    end

  end

end

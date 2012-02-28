
module Herdis

   module Handlers

    class Index < Goliath::API
      include Common

      def response(env)
        if Server.shepherd.nil?
          [404, {}, ""]
        else
          [200, {}, Server.shepherd.cluster_status]
        end
      end
      
    end

  end

end

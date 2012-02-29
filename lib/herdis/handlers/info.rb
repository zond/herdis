
module Herdis

   module Handlers

    class Info < Goliath::API
      include Common

      def response(env)
        if Server.shepherd.nil?
          [404, {}, ""]
        else
          [200, {}, Server.shepherd.cluster_info]
        end
      end
      
    end

  end

end

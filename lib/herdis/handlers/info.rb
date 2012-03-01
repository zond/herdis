
module Herdis

   module Handlers

    class Info < Goliath::API
      include Common

      def response(env)
        if Herdis::Plugins::ShepherdConnection.shepherd.nil?
          [404, {}, ""]
        else
          [200, {}, Herdis::Plugins::ShepherdConnection.shepherd.cluster_info]
        end
      end
      
    end

  end

end

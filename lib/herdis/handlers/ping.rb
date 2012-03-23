
module Herdis

   module Handlers

    class Ping < Goliath::API
      include Common

      def response(env)
        if Herdis::Plugins::ShepherdConnection.shepherd.nil?
          [404, {}, ""]
        else
          [Herdis::Plugins::ShepherdConnection.shepherd.status, {}, ""]
        end
      end
      
    end

  end

end

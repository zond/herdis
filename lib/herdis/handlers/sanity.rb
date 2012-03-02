
module Herdis

   module Handlers

    class Sanity < Goliath::API
      include Common

      def response(env)
        if Herdis::Plugins::ShepherdConnection.shepherd.nil?
          [404, {}, ""]
        else
          [200, {}, Herdis::Plugins::ShepherdConnection.shepherd.sanity]
        end
      end
      
    end

  end

end


module Herdis

   module Handlers

    class Shards < Goliath::API
      include Common

      def response(env)
        if Herdis::Plugins::ShepherdConnection.shepherd.nil?
          [404, {}, ""]
        else
          [200, {}, Herdis::Plugins::ShepherdConnection.shepherd.shard_status]
        end
      end
      
    end

  end

end

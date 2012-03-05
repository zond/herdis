
module Herdis

   module Handlers

    class RemoveShard < Goliath::API
      include Common

      def response(env)
        Herdis::Plugins::ShepherdConnection.shepherd.remove_shard(env['params'][:shepherd_id], env['params'][:shard_id])
        [204, {}, ""]
      end
      
    end

  end

end

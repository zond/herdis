
module Herdis

   module Handlers

    class RemoveShards < Goliath::API
      include Common

      def response(env)
        shard_ids = env['params']["shard_ids"]
        shepherd_id = env['params'][:shepherd_id]
        Herdis::Plugins::ShepherdConnection.shepherd.remove_shards(shepherd_id, shard_ids)
        [204, {}, ""]
      end
      
    end

  end

end

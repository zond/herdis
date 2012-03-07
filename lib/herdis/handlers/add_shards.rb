
module Herdis

   module Handlers

    class AddShards < Goliath::API
      include Common

      def response(env)
        shard_ids = env['params']["shard_ids"]
        shepherd_id = env['params'][:shepherd_id]
        Herdis::Plugins::ShepherdConnection.shepherd.add_shards(shepherd_id, shard_ids)
        [201, {}, ""]
      end
      
    end

  end

end

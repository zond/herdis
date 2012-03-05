
module Herdis

   module Handlers

    class AddShard < Goliath::API
      include Common

      def response(env)
        Herdis::Plugins::ShepherdConnection.shepherd.add_shard(env['params'][:shepherd_id], env['params'][:shard_id])
        [201, {}, ""]
      end
      
    end

  end

end

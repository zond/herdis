
module Herdis

   module Handlers

    class RemoveShepherd < Goliath::API
      include Common

      def response(env)
        Herdis::Plugins::ShepherdConnection.shepherd.remove_shepherd(env['params'][:shepherd_id])
        [204, {}, ""]
      end
      
    end

  end

end

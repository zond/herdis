
module Herdis

   module Handlers

    class RemoveShepherd < Goliath::API
      include Common

      def response(env)
        Herdis::Plugins::ShepherdConnection.shepherd.remove_shepherd(env['REQUEST_PATH'])
        [204, {}, ""]
      end
      
    end

  end

end

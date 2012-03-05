
module Herdis

   module Handlers

    class AddShepherd < Goliath::API
      include Common

      def response(env)
        data = env['params']
        data.delete(:shepherd_id)
        data.delete("shepherd_id")
        Herdis::Plugins::ShepherdConnection.shepherd.add_shepherd(data)
        [201, {}, ""]
      end
      
    end

  end

end


module Herdis

   module Handlers

    class Index < Goliath::API
      include Common

      def response(env)
        [200, {}, {:ping => Herdis::Shepherd.instance.ping}]
      end
      
    end

  end

end

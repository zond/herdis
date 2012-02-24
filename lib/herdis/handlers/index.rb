
module Herdis

   module Handlers

    class Index < Goliath::API
      include Common

      def response(env)
        instance = Herdis::Shepherd.instance
        [200, {}, instance.status]
      end
      
    end

  end

end

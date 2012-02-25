module Herdis
  module Rack
    class ShepherdNotifier
      include Goliath::Rack::AsyncMiddleware
      
      def call(env)
        class << Fiber.current
          attr_accessor :public_url
        end
        Fiber.current.public_url = URI.parse("http://#{env["SERVER_NAME"]}:#{env["SERVER_PORT"]}")
        super(env)
      end

    end
  end
end

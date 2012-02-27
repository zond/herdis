module Herdis
  module Rack
    class ShepherdNotifier
      include Goliath::Rack::AsyncMiddleware
      
      def call(env)
        class << Fiber.current
          attr_accessor :host
          attr_accessor :port
          attr_accessor :logger
        end
        Fiber.current.host = env["SERVER_NAME"]
        Fiber.current.port = env["SERVER_PORT"]
        Fiber.current.logger = env.logger
        super(env)
      end

    end
  end
end

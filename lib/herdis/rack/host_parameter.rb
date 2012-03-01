
module Herdis

  module Rack

    class HostParameter

      include Goliath::Rack::AsyncMiddleware

      def call(env)
        class << Fiber.current
          attr_accessor :host
        end
        Fiber.current.host = env["SERVER_NAME"]
        super(env)
      end

    end

  end

end

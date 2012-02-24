module Herdis
  module Rack
    class DefaultHeaders
      include Goliath::Rack::AsyncMiddleware

      DEFAULT_HEADERS = {
        'Content-Type' => 'application/json'
      }

      def post_process(env, status, headers, body)
        [status, headers.merge(DEFAULT_HEADERS), body]
      end
    end
  end
end

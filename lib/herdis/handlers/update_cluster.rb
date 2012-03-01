
module Herdis

   module Handlers

    class UpdateCluster < Goliath::API
      include Common

      def response(env)
        data = env['params']
        if data["type"] == "Shepherd"
          Herdis::Plugins::ShepherdConnection.shepherd.accept_shepherd(data)
        elsif data["type"] == "Cluster"
          Herdis::Plugins::ShepherdConnection.shepherd.merge_cluster(data)
        else
          raise Goliath::Validation::UnsupportedMediaTypeError.new(data["type"])
        end
        [200, {}, Herdis::Plugins::ShepherdConnection.shepherd.cluster_status]
      end
      
    end

  end

end

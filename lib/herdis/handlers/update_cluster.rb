
module Herdis

   module Handlers

    class UpdateCluster < Goliath::API
      include Common

      def response(env)
        data = env['params']
        if data["type"] == "Node"
          Server.shepherd.accept_node(data)
        elsif data["type"] == "Cluster"
          Server.shepherd.merge_cluster(data)
        else
          raise Goliath::Validation::UnsupportedMediaTypeError.new(data["type"])
        end
        [200, {}, Server.shepherd.cluster_status]
      end
      
    end

  end

end

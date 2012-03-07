
module Support
  
  module ServerSupport
    
    def start_server(port, pidfile, env = {})
      opts = ""
      env.each do |key, value|
        env_key = key.to_s.upcase
        env_key = "SHEPHERD_#{env_key}" unless env_key.index("SHEPHERD_") == 0
        opts += " #{env_key}=#{value}"
      end
      cmd = "env#{opts} #{File.expand_path('bin/herdis')} -p #{port} -d -P #{pidfile} -l #{File.join(File.dirname(__FILE__), "..", "..", "logs", "#{port}.log")}"
      system(cmd)    
    end
    
    def wait_for_server(port)
      EM.synchrony do
        status = 0
        while status == 0
          status = EM::HttpRequest.new("http://localhost:#{port}/info").get.response_header.status
          EM::Synchrony.sleep(0.5)
        end
        EM.stop
      end
    end
    
    def stop_server(port, pidfile, dir)
      EM.synchrony do
        EM::HttpRequest.new("http://localhost:#{port}/").delete rescue nil
        Process.kill("KILL", pidfile.read.to_i) rescue nil
        FileUtils.rm_r(dir) rescue nil
        EM.stop
      end
    end

    def redis_slave_of?(slaveport, masterport)
      ok = true
      begin
        info = Redis.new(:host => "localhost", :port => slaveport, :password => "slaved").info
        ok &= (info["role"] == "slave")
        ok &= (info["master_host"] == "localhost")
        ok &= (info["master_port"].to_i == masterport)
      rescue Errno::ECONNREFUSED => e
        ok = false
      rescue RuntimeError => e
        if e.message == "ERR operation not permitted"
          ok = false
        elsif e.message == "ERR Client sent AUTH, but no password is set"
          ok = false
        else
          raise e
        end
      end
      ok
    end

    def redis_dead?(port)
      begin
        Redis.new(:host => "127.0.0.1", :port => port).ping
        return false
      rescue Errno::ECONNREFUSED => e
        return true
      end
    end

    def redis_alive?(port)
      ok = true
      begin
        ok &= (Redis.new(:host => "127.0.0.1", :port => port).ping == "PONG")
      rescue Errno::ECONNREFUSED => e
        ok = false
      rescue RuntimeError => e
        if e.message == "ERR operation not permitted"
          ok = false
        else
          raise e
        end
      end
      ok
    end
    
    def redis_master?(masterport)
      ok = true
      begin
        ok &= (Redis.new(:host => "localhost", :port => masterport).info["role"] == "master")
      rescue Errno::ECONNREFUSED => e
        ok = false
      rescue RuntimeError => e
        if e.message == "ERR operation not permitted"
          ok = false
        else
          raise e
        end
      end
      ok
    end

    def stable_cluster?(http_port, *shepherd_ids)
      ok = true
      data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{http_port}/cluster").get.response)
      if data.nil?
        ok = false
      else
        Herdis::Common::SHARDS.times do |n|
          correct_shepherd_id = shepherd_ids[n % shepherd_ids.size]
          data.each do |shepherd_id, shepherd_stat|
            if shepherd_id == correct_shepherd_id
              ok &= data[shepherd_id]["masters"].include?(n.to_s)
            else
              ok &= !data[shepherd_id]["masters"].include?(n.to_s)
            end
          end
        end
      end
      if ok
        data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{http_port}/sanity").get.response)
        ok &= data["consistent"]
      end
      ok
    end

    def assert_stable_cluster(http_port, *shepherd_ids)
      assert_true_within(20) do
        stable_cluster?(http_port, *shepherd_ids)
      end
    end
    
    def assert_true_within(timeout, &block)
      deadline = Time.now.to_f + timeout
      rval = nil
      while (rval = block.call) == false
        EM::Synchrony.sleep 0.5
      end
      rval.should == true
    end

  end

end

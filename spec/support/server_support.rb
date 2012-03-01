
module Support
  
  module ServerSupport
    
    def start_server(port, pidfile, env = {})
      opts = ""
      env.each do |key, value|
        env_key = key.to_s.upcase
        env_key = "SHEPHERD_#{env_key}" unless env_key.index("SHEPHERD_") == 0
        opts += " #{env_key}=#{value}"
      end
      cmd = "env#{opts} #{File.expand_path('bin/herdis')} -p #{port} -d -P #{pidfile}"
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
        Process.kill("QUIT", pidfile.read.to_i) rescue nil
        FileUtils.rm_r(dir) rescue nil
        EM.stop
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

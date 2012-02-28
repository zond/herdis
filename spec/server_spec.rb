require File.expand_path('spec/spec_helper')

describe Herdis::Server do

  context 'starting up from scratch' do
    
    before :all do
      EM.synchrony do
        @dir = Dir.mktmpdir
        @pidfile = Tempfile.new("pid")
        @first_port = 11000
        @http_port = 12000
        @shepherd_id = rand(1 << 256).to_s(36)
        system("env SHEPHERD_DIR=#{@dir} SHEPHERD_FIRST_PORT=#{@first_port} SHEPHERD_ID=#{@shepherd_id} #{File.expand_path('bin/herdis')} -p #{@http_port} -d -P #{@pidfile.path}")
        EM.stop
      end
    end
    
    after :all do
      EM.synchrony do
        EM::HttpRequest.new("http://localhost:#{@http_port}/").delete rescue nil
        Process.kill("QUIT", @pidfile.read.to_i) rescue nil
        FileUtils.rm_r(@dir) rescue nil
        EM.stop
      end
    end
    
    it 'starts 128 redises at the provided port' do
      128.times do |n|
        Redis.new(:host => "127.0.0.1", :port => @first_port + n).ping.should == "PONG"
      end
    end
    
    it 'starts 128 redises in the provided directory' do
      128.times do |n|
        File.exists?(File.join(@dir, "shard#{n}", "pid")).should == true
      end
    end
    
    it 'has the provided shepherd_id on GET' do
      data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port}/").get.response)
      data["shepherds"].to_a[0][0].should == @shepherd_id
    end

    it 'broadcasts 128 shards on the given ports on GET' do
      data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port}/").get.response)
      data["shards"].size.should == 128
      128.times do |n|
        data["shards"]["#{n}"]["url"].should == "redis://localhost:#{@first_port + n}/"
      end
    end
    
    it 'shuts down all its redises on DELETE' do
      EM::HttpRequest.new("http://localhost:#{@http_port}/").delete
      128.times do |n|
        Proc.new do
          Redis.new(:host => "127.0.0.1", :port => @first_port + n).ping
        end.should raise_error
      end
    end
    
  end

  context 'joining each other' do

    context 'with real slow takeover' do

      before :all do
        EM.synchrony do
          @dir1 = Dir.mktmpdir
          @pidfile1 = Tempfile.new("pid")
          @first_port1 = 13000
          @http_port1 = 14000
          @shepherd_id1 = "id1"
          system("env SHEPHERD_CHECK_SLAVE_SHARDS=1000 SHEPHERD_INMEMORY=true SHEPHERD_DIR=#{@dir1} SHEPHERD_FIRST_PORT=#{@first_port1} SHEPHERD_ID=#{@shepherd_id1} #{File.expand_path('bin/herdis')} -p #{@http_port1} -d -P #{@pidfile1.path} -l /Users/zond/tmp/l1")
          @dir2 = Dir.mktmpdir
          @pidfile2 = Tempfile.new("pid")
          @first_port2 = 15000
          @http_port2 = 16000
          @shepherd_id2 = "id2"
          system("env SHEPHERD_CHECK_SLAVE_SHARDS=1000 SHEPHERD_INMEMORY=true SHEPHERD_DIR=#{@dir2} SHEPHERD_FIRST_PORT=#{@first_port2} SHEPHERD_ID=#{@shepherd_id2} #{File.expand_path('bin/herdis')} -p #{@http_port2} -d -P #{@pidfile2.path} -l /Users/zond/tmp/l2")
          EM::HttpRequest.new("http://localhost:#{@http_port2}/?url=#{CGI.escape("http://localhost:#{@http_port1}/")}").post.response
          EM.stop
        end
      end
      
      after :all do
        EM.synchrony do
          EM::HttpRequest.new("http://localhost:#{@http_port1}/").delete rescue nil
          Process.kill("QUIT", @pidfile1.read.to_i) rescue nil
          FileUtils.rm_r(@dir1) rescue nil
          EM::HttpRequest.new("http://localhost:#{@http_port2}/").delete rescue nil
          Process.kill("QUIT", @pidfile2.read.to_i) rescue nil
          FileUtils.rm_r(@dir2) rescue nil
          EM.stop
        end
      end
      
      it 'runs only the redises it owns after joining' do
        128.times do |n|
          Redis.new(:host => "127.0.0.1", :port => @first_port1 + n).ping.should == "PONG"
          if n % 2 == 0
            Proc.new do
              Redis.new(:host => "127.0.0.1", :port => @first_port2 + n).ping.should == "PONG"
            end.should raise_error(Errno::ECONNREFUSED)
          else
            Redis.new(:host => "127.0.0.1", :port => @first_port2 + n).ping.should == "PONG"
          end
        end
      end
      
      it 'gets included in the cluster state' do
        state1 = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port1}/").get.response)
        state2 = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port2}/").get.response)
        state1["shepherds"].keys.sort.should == ["id1", "id2"].sort
        state1["shepherds"].should == state2["shepherds"]
      end
      
      it 'gets the clusters existing shards' do
        state1 = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port1}/").get.response)
        state2 = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port2}/").get.response)
        128.times do |n|
          state1["shards"][n.to_s]["url"].should == "redis://localhost:#{@first_port1 + n}/"
        end
        state1["shards"].should == state2["shards"]
      end
      
      it 'starts slave shards for all shards in the cluster it should own' do
        128.times do |n|
          if n % 2 == 1
            r = Redis.new(:host => "localhost", :port => @first_port2 + n)
            info = r.info
            info["role"].should == "slave"
            info["master_host"].should == "localhost"
            info["master_port"].to_i.should == @first_port1 + n
          end
        end
      end

    end

    context 'with real fast takeover' do

      before :all do
        EM.synchrony do
          @dir1 = Dir.mktmpdir
          @pidfile1 = Tempfile.new("pid")
          @first_port1 = 13000
          @http_port1 = 14000
          @shepherd_id1 = "id1"
          system("env SHEPHERD_CHECK_SLAVE_TIMER=0.5 SHEPHERD_INMEMORY=true SHEPHERD_DIR=#{@dir1} SHEPHERD_FIRST_PORT=#{@first_port1} SHEPHERD_ID=#{@shepherd_id1} #{File.expand_path('bin/herdis')} -p #{@http_port1} -d -P #{@pidfile1.path} -l /Users/zond/tmp/l1")
          @dir2 = Dir.mktmpdir
          @pidfile2 = Tempfile.new("pid")
          @first_port2 = 15000
          @http_port2 = 16000
          @shepherd_id2 = "id2"
          system("env SHEPHERD_CHECK_SLAVE_TIMER=0.5 SHEPHERD_INMEMORY=true SHEPHERD_DIR=#{@dir2} SHEPHERD_FIRST_PORT=#{@first_port2} SHEPHERD_ID=#{@shepherd_id2} #{File.expand_path('bin/herdis')} -p #{@http_port2} -d -P #{@pidfile2.path} -l /Users/zond/tmp/l2")
          EM::HttpRequest.new("http://localhost:#{@http_port2}/?url=#{CGI.escape("http://localhost:#{@http_port1}/")}").post.response
          EM.stop
        end
      end
      
      after :all do
        EM.synchrony do
          EM::HttpRequest.new("http://localhost:#{@http_port1}/").delete rescue nil
          Process.kill("QUIT", @pidfile1.read.to_i) rescue nil
          FileUtils.rm_r(@dir1) rescue nil
          EM::HttpRequest.new("http://localhost:#{@http_port2}/").delete rescue nil
          Process.kill("QUIT", @pidfile2.read.to_i) rescue nil
          FileUtils.rm_r(@dir2) rescue nil
          EM.stop
        end
      end
      
      it 'shuts down its non-owned master shards when they are broadcast from their owner' do
        proper_redises_running = nil
        100.times do
          proper_redises_running = true
          128.times do |n|
            if n % 2 == 0
              begin
                proper_redises_running &= Redis.new(:host => "localhost", :port => @first_port1 + n).ping == "PONG"
              rescue Errno::ECONNREFUSED => e
                puts "no redis running on #{@first_port1 + n}!"
                proper_redises_running = false
              end
              begin
                Redis.new(:host => "localhost", :port => @first_port2 + n).ping
                proper_redises_running = false
              rescue Errno::ECONNREFUSED => e
                proper_redises_running &= true
              end
            else
              begin
                proper_redises_running &= Redis.new(:host => "localhost", :port => @first_port2 + n).ping == "PONG"
              rescue Errno::ECONNREFUSED => e
                puts "no redis running on #{@first_port2 + n}!"
                proper_redises_running = false
              end
              begin
                Redis.new(:host => "localhost", :port => @first_port1 + n).ping
                proper_redises_running = false
              rescue Errno::ECONNREFUSED => e
                proper_redises_running &= true
              end
            end
          end
          break if proper_redises_running
          EM::Synchrony.sleep 0.5
        end
        proper_redises_running.should == true
      end

      it 'makes its slave shards masters when the master shards disappear' do
        proper_ownership = nil
        data = nil
        begin
          100.times do
            data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port1}/").get.response)
            proper_ownership = true
            128.times do |n|
              if n % 2 == 0
                proper_ownership &= data["shards"][n.to_s]["url"] == "redis://localhost:#{@first_port1 + n}/"
              else
                proper_ownership &= data["shards"][n.to_s]["url"] == "redis://localhost:#{@first_port2 + n}/"
              end
            end
            break if proper_ownership
            EM::Synchrony.sleep 0.5
          end
          pp data unless proper_ownership
          proper_ownership.should == true
          100.times do
            data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port2}/").get.response)
            proper_ownership = true
            128.times do |n|
              if n % 2 == 0
                proper_ownership &= data["shards"][n.to_s]["url"] == "redis://localhost:#{@first_port1 + n}/"
              else
                proper_ownership &= data["shards"][n.to_s]["url"] == "redis://localhost:#{@first_port2 + n}/"
              end
            end
            break if proper_ownership
            EM::Synchrony.sleep 0.5
          end
          pp data unless proper_ownership
          proper_ownership.should == true
        rescue Exception => e
          pp data
          raise e
        end
      end
      
    end

  end

  context 'when being in a cluster' do
    
    it 'regularly pings its predecessor to make sure it is alive'

    it 'broadcasts a new cluster state if the predecessor doesnt respond'

    it 'backs up N predecessors using slave shards'

    it 'broadcasts its backup shards as master shards when the old master shards disappear'

  end

end

require File.expand_path('spec/spec_helper')

describe Herdis::Server do

  context 'starting up from scratch' do
    
    before :all do
      @dir = Dir.mktmpdir
      @pidfile = Tempfile.new("pid")
      @first_port = 11000
      @http_port = 12000
      @shepherd_id = rand(1 << 256).to_s(36)
      start_server(@http_port, 
                   @pidfile.path, 
                   :inmemory => true, 
                   :dir => @dir, 
                   :first_port => @first_port, 
                   :shepherd_id => @shepherd_id)
      wait_for_server(@http_port)
    end
    
    after :all do
      stop_server(@http_port, @pidfile, @dir)
    end
    
    it "starts #{Herdis::Common::SHARDS} redises at the provided port" do
      Herdis::Common::SHARDS.times do |n|
        Redis.new(:host => "127.0.0.1", :port => @first_port + n).ping.should == "PONG"
      end
    end
    
    it "starts #{Herdis::Common::SHARDS} redises in the provided directory" do
      Herdis::Common::SHARDS.times do |n|
        File.exists?(File.join(@dir, "shard#{n}", "pid")).should == true
      end
    end
    
    it 'has the provided shepherd_id on GET' do
      data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port}/cluster").get.response)
      data.to_a[0][0].should == @shepherd_id
    end

    it "broadcasts #{Herdis::Common::SHARDS} shards on the given ports on GET" do
      data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port}/cluster").get.response)
      data[@shepherd_id]["masters"].size.should == Herdis::Common::SHARDS
      Herdis::Common::SHARDS.times do |n|
        data[@shepherd_id]["masters"].include?("#{n}").should == true
      end
    end
    
    it 'shuts down all its redises on DELETE' do
      EM::HttpRequest.new("http://localhost:#{@http_port}/").delete
      Herdis::Common::SHARDS.times do |n|
        Proc.new do
          Redis.new(:host => "127.0.0.1", :port => @first_port + n).ping
        end.should raise_error
      end
    end
    
  end

  context 'in a cluster' do
    
    context 'with redunancy 1' do
      
      before :all do
        @dir1 = Dir.mktmpdir
        @pidfile1 = Tempfile.new("pid")
        @first_port1 = 13000
        @http_port1 = 14000
        @shepherd_id1 = "id1"
        start_server(@http_port1, 
                     @pidfile1.path, 
                     :redundancy => 1, 
                     :check_slave_timer => 0.5,
                     :dir => @dir1, 
                     :first_port => @first_port1, 
                     :shepherd_id => @shepherd_id1)
        wait_for_server(@http_port1)
        @dir2 = Dir.mktmpdir
        @pidfile2 = Tempfile.new("pid")
        @first_port2 = 15000
        @http_port2 = 16000
        @shepherd_id2 = "id2"
        start_server(@http_port2, 
                     @pidfile2.path, 
                     :connect_to => "http://localhost:#{@http_port1}",
                     :redundancy => 1, 
                     :check_slave_timer => 0.5,
                     :dir => @dir2, 
                     :first_port => @first_port2, 
                     :shepherd_id => @shepherd_id2)
        wait_for_server(@http_port2)
      end
      
      after :all do
        stop_server(@http_port1, @pidfile1, @dir1)
        stop_server(@http_port2, @pidfile2, @dir2)
      end

      it 'backs up 1 predecessor using slave shards' do
        assert_true_within(20) do
          proper_redises_running = true
          Herdis::Common::SHARDS.times do |n|
            if n % 2 == 0
              begin
                proper_redises_running &= (Redis.new(:host => "localhost", :port => @first_port1 + n).info["role"] == "master")
              rescue Errno::ECONNREFUSED => e
                proper_redises_running = false
              rescue RuntimeError => e
                if e.message == "ERR operation not permitted"
                  proper_redises_running &= false
                else
                  raise e
                end
              end
              begin
                info = Redis.new(:host => "localhost", :port => @first_port2 + n, :password => "slaved").info
                proper_redises_running &= (info["role"] == "slave")
                proper_redises_running &= (info["master_host"] == "localhost")
                proper_redises_running &= (info["master_port"].to_i == @first_port1 + n)
              rescue Errno::ECONNREFUSED => e
                proper_redises_running &= false
              rescue RuntimeError => e
                if e.message == "ERR operation not permitted"
                  proper_redises_running &= false
                else
                  raise e
                end
              end
            else
              begin
                proper_redises_running &= (Redis.new(:host => "localhost", :port => @first_port2 + n).info["role"] == "master")
              rescue Errno::ECONNREFUSED => e
                proper_redises_running = false
              rescue RuntimeError => e
                if e.message == "ERR operation not permitted"
                  proper_redises_running &= false
                else
                  raise e
                end
              end
              begin
                info = Redis.new(:host => "localhost", :port => @first_port1 + n, :password => "slaved").info
                proper_redises_running &= (info["role"] == "slave")
                proper_redises_running &= (info["master_host"] == "localhost")
                proper_redises_running &= (info["master_port"].to_i == @first_port2 + n)
              rescue Errno::ECONNREFUSED => e
                proper_redises_running &= false
              rescue RuntimeError => e
                if e.message == "ERR operation not permitted"
                  proper_redises_running &= false
                else
                  raise e
                end
              end
            end
          end
          proper_redises_running
        end
      end
      
    end

    context 'without redundancy' do
      
      context 'with real slow takeover' do
        
        before :all do
          @dir1 = Dir.mktmpdir
          @pidfile1 = Tempfile.new("pid")
          @first_port1 = 13000
          @http_port1 = 14000
          @shepherd_id1 = "id1"
          start_server(@http_port1, 
                       @pidfile1.path, 
                       :check_slave_timer => 10000,
                       :shepherd_redundancy => 0, 
                       :dir => @dir1, 
                       :first_port => @first_port1, 
                       :shepherd_id => @shepherd_id1)
          wait_for_server(@http_port1)
          @dir2 = Dir.mktmpdir
          @pidfile2 = Tempfile.new("pid")
          @first_port2 = 15000
          @http_port2 = 16000
          @shepherd_id2 = "id2"
          start_server(@http_port2, 
                       @pidfile2.path, 
                       :check_slave_timer => 10000,
                       :connect_to => "http://localhost:#{@http_port1}",
                       :shepherd_redundancy => 0, 
                       :dir => @dir2, 
                       :first_port => @first_port2, 
                       :shepherd_id => @shepherd_id2)
          wait_for_server(@http_port2)
        end
        
        after :all do
          stop_server(@http_port1, @pidfile1, @dir1)
          stop_server(@http_port2, @pidfile2, @dir2)
        end
        
        it 'runs only the redises it owns after joining' do
          assert_true_within(20) do
            ok = true
            Herdis::Common::SHARDS.times do |n|
              Redis.new(:host => "127.0.0.1", :port => @first_port1 + n).ping.should == "PONG"
              if n % 2 == 0
                begin
                  Redis.new(:host => "127.0.0.1", :port => @first_port2 + n).ping.should == "PONG"
                  ok = false
                rescue Errno::ECONNREFUSED => e
                end
              else
                begin
                  Redis.new(:host => "127.0.0.1", :port => @first_port2 + n, :password => "slaved").ping.should == "PONG"                  
                rescue Errno::ECONNREFUSED => e
                  ok = false
                rescue RuntimeError => e
                  if e.message == "ERR operation not permitted"
                    ok = false
                  else
                    raise e
                  end
                end
              end
            end
            ok
          end
        end
        
        it 'gets included in the cluster state' do
          state1 = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port1}/cluster").get.response)
          state2 = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port2}/cluster").get.response)
          state1.keys.sort.should == ["id1", "id2"].sort
          state1.should == state2
        end
        
        it 'gets the clusters existing shards' do
          state1 = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port1}/cluster").get.response)
          state2 = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port2}/cluster").get.response)
          Herdis::Common::SHARDS.times do |n|
            state1[@shepherd_id1]["masters"].include?(n.to_s).should == true
          end
          state1.should == state2
        end
        
        it 'starts slave shards for all shards in the cluster it should own' do
          Herdis::Common::SHARDS.times do |n|
            if n % 2 == 1
              r = Redis.new(:host => "localhost", :port => @first_port2 + n, :password => "slaved")
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
          @dir1 = Dir.mktmpdir
          @pidfile1 = Tempfile.new("pid")
          @first_port1 = 13000
          @http_port1 = 14000
          @shepherd_id1 = "id1"
          start_server(@http_port1, 
                       @pidfile1.path, 
                       :check_slave_timer => 0.5,
                       :shepherd_redundancy => 0, 
                       :dir => @dir1, 
                       :first_port => @first_port1, 
                       :shepherd_id => @shepherd_id1)
          wait_for_server(@http_port1)
          @dir2 = Dir.mktmpdir
          @pidfile2 = Tempfile.new("pid")
          @first_port2 = 15000
          @http_port2 = 16000
          @shepherd_id2 = "id2"
          start_server(@http_port2, 
                       @pidfile2.path, 
                       :check_slave_timer => 0.5,
                       :connect_to => "http://localhost:#{@http_port1}",
                       :shepherd_redundancy => 0, 
                       :dir => @dir2, 
                       :first_port => @first_port2, 
                       :shepherd_id => @shepherd_id2)
          wait_for_server(@http_port2)
        end
        
        after :all do
          stop_server(@http_port1, @pidfile1, @dir1)
          stop_server(@http_port2, @pidfile2, @dir2)
        end
        
        it 'shuts down its non-owned master shards when they are broadcast from their owner' do
          assert_true_within(20) do
            proper_redises_running = true
            Herdis::Common::SHARDS.times do |n|
              if n % 2 == 0
                begin
                  proper_redises_running &= (Redis.new(:host => "localhost", :port => @first_port1 + n).ping == "PONG")
                rescue Errno::ECONNREFUSED => e
                  proper_redises_running = false
                end
                begin
                  Redis.new(:host => "localhost", :port => @first_port2 + n).ping
                  proper_redises_running = false
                rescue Errno::ECONNREFUSED => e
                end
              else
                begin
                  proper_redises_running &= (Redis.new(:host => "localhost", :port => @first_port2 + n).ping == "PONG")
                rescue Errno::ECONNREFUSED => e
                  proper_redises_running = false
                end
                begin
                  Redis.new(:host => "localhost", :port => @first_port1 + n).ping
                  proper_redises_running = false
                rescue Errno::ECONNREFUSED => e
                end
              end
            end
            proper_redises_running
          end
        end

        it 'makes its slave shards masters when the master shards disappear' do
          assert_true_within(20) do
            proper_ownership = true
            data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port1}/shards").get.response)
            Herdis::Common::SHARDS.times do |n|
              if n % 2 == 0
                proper_ownership &= data["shards"][n.to_s]["url"] == "redis://localhost:#{@first_port1 + n}/"
              else
                proper_ownership &= data["shards"][n.to_s]["url"] == "redis://localhost:#{@first_port2 + n}/"
              end
            end
            pp data unless proper_ownership
            proper_ownership
          end
          assert_true_within(20) do
            proper_ownership = true
            data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port2}/shards").get.response)
            Herdis::Common::SHARDS.times do |n|
              if n % 2 == 0
                proper_ownership &= data["shards"][n.to_s]["url"] == "redis://localhost:#{@first_port1 + n}/"
              else
                proper_ownership &= data["shards"][n.to_s]["url"] == "redis://localhost:#{@first_port2 + n}/"
              end
            end
            pp data unless proper_ownership
            proper_ownership
          end
        end

      end
        
    end

  end

  context 'when the cluster crashes' do
    
    before :all do
      @dir1 = Dir.mktmpdir
      @pidfile1 = Tempfile.new("pid")
      @first_port1 = 13000
      @http_port1 = 14000
      @shepherd_id1 = "id1"
      start_server(@http_port1, 
                   @pidfile1.path, 
                   :check_slave_timer => 0.5,
                   :shepherd_redundancy => 1, 
                   :dir => @dir1, 
                   :first_port => @first_port1, 
                   :shepherd_id => @shepherd_id1)
      wait_for_server(@http_port1)
      @dir2 = Dir.mktmpdir
      @pidfile2 = Tempfile.new("pid")
      @first_port2 = 15000
      @http_port2 = 16000
      @shepherd_id2 = "id2"
      start_server(@http_port2, 
                   @pidfile2.path, 
                   :check_slave_timer => 0.5,
                   :connect_to => "http://localhost:#{@http_port1}",
                   :shepherd_redundancy => 1, 
                   :dir => @dir2, 
                   :first_port => @first_port2, 
                   :shepherd_id => @shepherd_id2)
      wait_for_server(@http_port2)
      @dir3 = Dir.mktmpdir
      @pidfile3 = Tempfile.new("pid")
      @first_port3 = 17000
      @http_port3 = 18000
      @shepherd_id3 = "id3"
      start_server(@http_port3, 
                   @pidfile3.path, 
                   :check_slave_timer => 0.5,
                   :connect_to => "http://localhost:#{@http_port1}",
                   :shepherd_redundancy => 1, 
                   :dir => @dir3, 
                   :first_port => @first_port3, 
                   :shepherd_id => @shepherd_id3)
      wait_for_server(@http_port3)
      EM.synchrony do
        p = [@first_port1, @first_port2, @first_port3]
        assert_true_within(20) do
          ok = true
          data = Yajl::Parser.parse(EM::HttpRequest.new("http://localhost:#{@http_port1}/shards").get.response)
          Herdis::Common::SHARDS.times do |n|
            port = p[n % p.size] + n
            if data["shards"][n.to_s]["url"] != "redis://localhost:#{port}/"
              ok = false
            end
          end
          ok
        end
      end
    end
    
    after :all do
      stop_server(@http_port1, @pidfile1, @dir1)
      stop_server(@http_port2, @pidfile2, @dir2)
      stop_server(@http_port3, @pidfile3, @dir3)
    end
    
    it 'regularly pings its predecessor and broadcasts a new cluster state if the predecessor doesnt respond' do
      puts "ok"
    end

    it 'broadcasts its backup shards as master shards when the old master shards disappear'

  end

end

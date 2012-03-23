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
              proper_redises_running &= redis_master?(@first_port1 + n)
              proper_redises_running &= redis_slave_of?(@first_port2 + n, @first_port1 + n)
            else
              proper_redises_running &= redis_master?(@first_port2 + n)
              proper_redises_running &= redis_slave_of?(@first_port1 + n, @first_port2 + n)
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
              ok &= redis_alive?(@first_port1 + n)
              if n % 2 == 0
                ok &= redis_dead?(@first_port2 + n)
              else
                ok &= redis_slave_of?(@first_port2 + n, @first_port1 + n)
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
              redis_slave_of?(@first_port2 + n, @first_port1 + n).should == true
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
                proper_redises_running &= redis_alive?(@first_port1 + n)
                proper_redises_running &= redis_dead?(@first_port2 + n)
              else
                proper_redises_running &= redis_dead?(@first_port1 + n)
                proper_redises_running &= redis_alive?(@first_port2 + n)
              end
            end
            proper_redises_running
          end
        end

        it 'makes its slave shards masters when the master shards disappear' do
          assert_stable_cluster(@http_port1, @shepherd_id1, @shepherd_id2)
          assert_stable_cluster(@http_port2, @shepherd_id1, @shepherd_id2)
        end

      end
        
    end

  end

  context 'when the cluster restarts' do
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
      EM.synchrony do
        assert_stable_cluster(@http_port1, @shepherd_id1, @shepherd_id2)
        EM.stop
      end
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
        assert_stable_cluster(@http_port1, @shepherd_id1, @shepherd_id2, @shepherd_id3)
        assert_stable_cluster(@http_port2, @shepherd_id1, @shepherd_id2, @shepherd_id3)
        assert_stable_cluster(@http_port3, @shepherd_id1, @shepherd_id2, @shepherd_id3)
        client = Herdis::Client.new("http://localhost:#{@http_port1}")
        @secret = rand(1 << 128)
        1000.times do |n|
          client.set("key:#{@secret}:#{n}", "value:#{@secret}:#{n}")
        end
        EM.stop
      end
      stop_cluster(@http_port1, @http_port1, @http_port2, @http_port3)
      Process.kill("KILL", @pidfile1.read.to_i) rescue nil
      Process.kill("KILL", @pidfile2.read.to_i) rescue nil
      Process.kill("KILL", @pidfile3.read.to_i) rescue nil
      start_server(@http_port1, 
                   @pidfile1.path, 
                   :check_slave_timer => 0.5,
                   :restart => "true",
                   :shepherd_redundancy => 1, 
                   :dir => @dir1, 
                   :first_port => @first_port1, 
                   :shepherd_id => @shepherd_id1)
      wait_for_server(@http_port1)
      start_server(@http_port2, 
                   @pidfile2.path, 
                   :check_slave_timer => 0.5,
                   :connect_to => "http://localhost:#{@http_port1}",
                   :restart => "true",
                   :shepherd_redundancy => 1, 
                   :dir => @dir2, 
                   :first_port => @first_port2, 
                   :shepherd_id => @shepherd_id2)
      wait_for_server(@http_port2)
      start_server(@http_port3, 
                   @pidfile3.path, 
                   :restart => "true",
                   :check_slave_timer => 0.5,
                   :connect_to => "http://localhost:#{@http_port1}",
                   :shepherd_redundancy => 1, 
                   :dir => @dir3, 
                   :first_port => @first_port3, 
                   :shepherd_id => @shepherd_id3)
      wait_for_server(@http_port3)
      EM.synchrony do
        assert_stable_cluster(@http_port1, @shepherd_id1, @shepherd_id2, @shepherd_id3)
        assert_stable_cluster(@http_port2, @shepherd_id1, @shepherd_id2, @shepherd_id3)
        assert_stable_cluster(@http_port3, @shepherd_id1, @shepherd_id2, @shepherd_id3)
        EM.stop
      end
    end
    after :all do
      stop_server(@http_port1, @pidfile1, @dir1)
      stop_server(@http_port2, @pidfile2, @dir2)
      stop_server(@http_port3, @pidfile3, @dir3)
    end

    it 'retains all data the cluster had before the crash' do
      client = Herdis::Client.new("http://localhost:#{@http_port1}")
      1000.times do |n|
        client.get("key:#{@secret}:#{n}").should == "value:#{@secret}:#{n}"
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
        assert_stable_cluster(@http_port1, @shepherd_id1, @shepherd_id2, @shepherd_id3)
        assert_stable_cluster(@http_port2, @shepherd_id1, @shepherd_id2, @shepherd_id3)
        assert_stable_cluster(@http_port3, @shepherd_id1, @shepherd_id2, @shepherd_id3)
        EM.stop
      end
      stop_server(@http_port3, @pidfile3, @dir3)
    end
    
    after :all do
      stop_server(@http_port1, @pidfile1, @dir1)
      stop_server(@http_port2, @pidfile2, @dir2)
    end
    
    it 'regularly pings its predecessor and broadcasts a new cluster state if the predecessor doesnt respond' do
      assert_stable_cluster(@http_port1, @shepherd_id1, @shepherd_id2)
      assert_stable_cluster(@http_port2, @shepherd_id1, @shepherd_id2)
    end
    
  end

end

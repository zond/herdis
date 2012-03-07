require File.expand_path('spec/spec_helper')

describe Herdis::Client do

  context 'regular operation' do
    
    before :all do
      @dir = Dir.mktmpdir
      @pidfile = Tempfile.new("pid")
      @first_port = 11500
      @http_port = 12500
      @shepherd_id = rand(1 << 256).to_s(36)
      start_server(@http_port, 
                   @pidfile.path, 
                   :inmemory => true, 
                   :dir => @dir, 
                   :first_port => @first_port, 
                   :shepherd_id => @shepherd_id)
      wait_for_server(@http_port)
      EM.synchrony do
        @client = Herdis::Client.new("http://localhost:#{@http_port}/")
        EM.stop
      end
    end
    
    after :all do
      stop_server(@http_port, @pidfile, @dir)
    end
    
    it 'works as a regular redis-rb client' do
      EM.synchrony do
        @client.get("x").should == nil
        @client.set("x", "y").should == "OK"
        @client.get("x").should == "y"
        EM.stop
      end
    end
    
  end

  context 'when the cluster changes' do
    
    before :all do
      @dir1 = Dir.mktmpdir
      @pidfile1 = Tempfile.new("pid")
      @first_port1 = 13500
      @http_port1 = 14500
      @shepherd_id1 = "id1"
      start_server(@http_port1, 
                   @pidfile1.path, 
                   :redundancy => 1, 
                   :check_slave_timer => 0.5,
                   :dir => @dir1, 
                   :first_port => @first_port1, 
                   :shepherd_id => @shepherd_id1)
      wait_for_server(@http_port1)
      EM.synchrony do
        @client = Herdis::Client.new("http://localhost:#{@http_port1}/")
        EM.stop
      end
    end

    after :all do
      stop_server(@http_port1, @pidfile1, @dir1)
      stop_server(@http_port2, @pidfile2, @dir2)
    end
    
    it 'still returns the same data after the cluster grows' do
      EM.synchrony do
        100.times do |n|
          @client.set("key#{n}", "value#{n}")
        end
        100.times do |n|
          @client.get("key#{n}").should == "value#{n}"
        end
        EM.stop
      end
      @dir2 = Dir.mktmpdir
      @pidfile2 = Tempfile.new("pid")
      @first_port2 = 15500
      @http_port2 = 16500
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
      EM.synchrony do
        100.times do |n|
          @client.get("key#{n}").should == "value#{n}"
        end
        EM.stop
      end
      assert_stable_cluster(@http_port1, @shepherd_id1, @shepherd_id2)
      assert_stable_cluster(@http_port2, @shepherd_id1, @shepherd_id2)
      EM.synchrony do
        100.times do |n|
          @client.get("key#{n}").should == "value#{n}"
        end
        EM.stop
      end
    end
    
  end

end

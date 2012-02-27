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
        `env SHEPHERD_DIR=#{@dir} SHEPHERD_FIRST_PORT=#{@first_port} SHEPHERD_ID=#{@shepherd_id} #{File.expand_path('bin/herdis')} -p #{@http_port} -d -P #{@pidfile.path}`
        EM.stop
      end
    end
    
    after :all do
      EM.synchrony do
        EM::HttpRequest.new("http://localhost:#{@http_port}/").delete
        Process.kill("QUIT", @pidfile.read.to_i)
        FileUtils.rm_r(@dir)
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
        data["shards"]["#{n}"].should == "redis://localhost:#{@first_port + n}/"
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

  context 'joining an existing shepherd' do

    before :all do
    end

    after :all do
    end

    it 'shuts down all its own redists prior to joining'

    it 'gets included in the cluster state'

    it 'gets the clusters existing shards'

    it 'starts slave shards for all shards in in the cluster it should own'

    it 'broadcasts its slave shards as for shards it should own when they are synced'

    it 'makes its slave shards masters when the master shards disappear'

  end

  context 'when being joined by a new shepherd' do
    
    it 'accepts the new shepherd'

    it 'broadcasts the new cluster state'

    it 'shuts down its non-owned master shards when they are broadcasted from their owner'

  end

  context 'when being in a cluster' do
    
    it 'regularly pings its predecessor to make sure it is alive'

    it 'broadcasts a new cluster state if the predecessor doesnt respond'

    it 'backs up N predecessors using slave shards'

    it 'broadcasts its backup shards as master shards when the old master shards disappear'

  end

end

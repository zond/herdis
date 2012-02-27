require File.expand_path('spec/spec_helper')

describe Herdis::Shepherd do

  context 'starting up from scratch' do

    before :all do
      @dir = Dir.mktmpdir
      @first_port = 11000
      @shepherd_id = rand(1 << 256).to_s(36)
      @shepherd = Herdis::Shepherd.new(:dir => @dir, :first_port => @first_port, :shepherd_id => @shepherd_id)
    end

    after :all do
      @shepherd.shutdown
      FileUtils.rm_r(@dir)
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

    it 'has the provided shepherd_id' do
      @shepherd.shepherd_id.should == @shepherd_id
    end
   
  end

  context 'joining an existing shepherd' do

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

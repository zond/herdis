require File.expand_path('spec/spec_helper')

describe Herdis::Shepherd do

  context 'running serverless' do

    context 'starting up from scratch' do

      before :all do
        EM.synchrony do
          @dir = Dir.mktmpdir
          @first_port = 11000
          @shepherd_id = rand(1 << 256).to_s(36)
          @shepherd = Herdis::Shepherd.new(:dir => @dir, :first_port => @first_port, :shepherd_id => @shepherd_id)
          EM.stop
        end
      end
      
      after :all do
        @shepherd.shutdown
        FileUtils.rm_r(@dir)
      end
      
      it "starts #{Herdis::Common::SHARDS} redises at the provided port" do
        Herdis::Common::SHARDS.times do |n|
          Redis.new(:host => "127.0.0.1", :port => @first_port + n).ping.should == "PONG"
        end
      end
      
      it 'starts #{Herdis::Common::SHARDS} redises in the provided directory' do
        Herdis::Common::SHARDS.times do |n|
          File.exists?(File.join(@dir, "shard#{n}", "pid")).should == true
        end
      end
      
      it 'has the provided shepherd_id' do
        @shepherd.shepherd_id.should == @shepherd_id
      end
      
      it 'shuts down all its redises on #shutdown' do
        @shepherd.shutdown
        Herdis::Common::SHARDS.times do |n|
          Proc.new do
            Redis.new(:host => "127.0.0.1", :port => @first_port + n).ping
          end.should raise_error
        end
      end

    end

  end

end

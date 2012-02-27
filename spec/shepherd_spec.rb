require File.expand_path('spec/spec_helper')

describe Herdis::Shepherd do

  context 'running serverless' do

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
      
      it 'shuts down all its redises on #shutdown' do
        @shepherd.shutdown
        128.times do |n|
          Proc.new do
            Redis.new(:host => "127.0.0.1", :port => @first_port + n).ping
          end.should raise_error
        end
      end

    end

  end

end
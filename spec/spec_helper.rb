
dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift dir + '/../lib'

require 'tmpdir'
require 'rspec'
require 'goliath'
require 'goliath/test_helper'
require 'herdis/server'
require 'herdis/client'
require 'fileutils'

$LOAD_PATH.unshift dir

require 'support/server_support'
require 'support/event_machine_helpers'

Goliath.env = :test

RSpec.configure do |c|
  c.around(:each) do |example|
    EM.synchrony do
      example.run
      EM.stop
    end
  end
  c.include Support::ServerSupport
  c.include Goliath::TestHelper
  c.extend EventMachineHelpers
end

module Goliath
  module TestHelper
    def server(api, port, options = {}, &blk)
      op = OptionParser.new

      s = Goliath::Server.new
      s.logger = herdis_get_logger
      s.api = api.new
      s.app = Goliath::Rack::Builder.build(api, s.api)
      s.api.options_parser(op, options)
      s.options = options
      s.port = @test_server_port = port
      s.start(&blk)
      s
    end
    def herdis_get_logger
      log = Log4r::Logger.new('goliath')
      log_format = Log4r::PatternFormatter.new(:pattern => "[#{Process.pid}:%l] %d :: %m")
      log.level = Log4r::WARN
      log.add(Log4r::StdoutOutputter.new('console', :formatter => log_format))
      log
    end
  end
end


require 'em-synchrony'
require 'em-synchrony/em-http'
require 'hiredis'
require 'redis'
require 'goliath'
require 'yajl'
require 'pp'

$LOAD_PATH.unshift(File.expand_path('lib'))

require 'rmerge'
require 'herdis/shepherd'
require 'herdis/server'

Goliath::Application.app_class = Herdis::Server


require 'hiredis'
require 'em-synchrony'
require 'em-http-request'
require 'redis'
require 'goliath'
require 'yajl'

$LOAD_PATH.unshift(File.expand_path('lib'))

require 'herdis/shepherd'
require 'herdis/server'

Goliath::Application.app_class = Herdis::Server

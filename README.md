# Herdis

Herdis is a simplistic Redis cluster manager based on 
[Redis Presharding](http://antirez.com/post/redis-presharding.html) described in antirez blog.

## Installation

### Ruby

We use Ruby 1.9.3. 

To install and run on your local machine use [RVM](https://rvm.beginrescueend.com/). 
For Mac machines make sure you compile with gcc-4.2 (because the compiler from Xcode doesn't compile Ruby 1.9.3 properly). 
Download and install gcc from https://github.com/kennethreitz/osx-gcc-installer 

    $ gem install rvm
    $ rvm install 1.9.3

And for Macs

    $ rvm install 1.9.3 --with-gcc=gcc-4.2

### Rubygems

Use [Bundler](http://gembundler.com/) to install the gems needed by Herdis

    $ bundle install

### Redis

Herdis naturally needs [Redis](http://redis.io/) to run. Install it and put redis-server in your $PATH.

## Running

The `bin/herdis` script runs a [Goliath](https://github.com/postrank-labs/goliath/) server with standard Goliath
parameters such as `-p` for port selection, `-s` for logging to STDOUT, `-v` for verbose logging etc.

The server also uses a set of ENV variables to control its configuration: 

* `SHEPHERD_FIRST_PORT` for the first port to run Redis instances on. Defaults to `9080`.
* `SHEPHERD_DIR` for the directory to put Redis dumps and pidfiles in. Defaults to `$HOME/.herdis`.
* `SHEPHERD_ID` for the id of the server to start. Defaults to a random string.
* `SHEPHERD_INMEMORY` to run a server managing only non-persistent Redis instances. Defaults to `false`
* `SHEPHERD_REDUNDANCY` to define number of backups to maintain of each shard. Defaults to `2`.
* `SHEPHERD_CONNECT_TO` to define another server to connect to on startup. Defaults to `nil`.

To actually start the server, simply

    $ bin/herdis

## Using

To use it you `require 'herdis/client'` and then instantiate a `Herdis::Client` which will act just like a `Redis::Distributed`.

    require 'herdis/client'

    client = Herdis::Client.new("http://localhost:9000")
    client.set("test", "value")
    raise "this should work, for example" unless client.get("test") == "value"

If you have a cluster of herdis nodes, you can give all their addresses to the new `Herdis::Client` when you start it (`Herdis::Client.new(url1, url2, url3... urln)`), that way it will keep trying until it finds one that works (if you want to be able to restart your clients even if the cluster is currently maimed).

## Test suite

    $ rake

## Console

To run an eventmachine-friendly console to test your servers from IRB

    $ bundle exec em-console

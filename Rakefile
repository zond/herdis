
$LOAD_PATH.unshift(File.expand_path('lib'))

require 'rake'
require 'bundler/setup'

unless Bundler.rubygems.find_name('rspec').empty?

  require 'rspec/core/rake_task'

  task :default => [:spec]
  task :test => [:spec]

  RSpec::Core::RakeTask.new(:spec) do |t|
    t.rspec_opts = '--color'
    t.pattern = 'spec/**/*_spec.rb'
  end
  
end


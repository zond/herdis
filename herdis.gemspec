Gem::Specification.new do |s|
  s.name      =   "herdis"
  s.version   =   "0.0.7"
  s.summary   =   "A Redis herder for simplifying Redis presharding"
  s.description = <<EOF
A Redis herder for simplifying Redis presharding
EOF
  s.author    = "Martin Bruse"
  s.email     = "zondolfin at gmail dot com"
  s.homepage  = "http://github.com/zond/herdis"

  s.require_path  = "lib"

  s.files     =   Dir.glob('lib/**/*.rb') + ['README.md'] + ['assets/shepherd.png']
  s.executables = ['herdis']

  s.has_rdoc  =   true
  s.rdoc_options << '--line-numbers'
  s.rdoc_options << '--inline-source'

  s.add_dependency('hiredis')
  s.add_dependency('em-synchrony')
  s.add_dependency('em-http-request')
  s.add_dependency('redis')
  s.add_dependency('yajl-ruby')
  s.add_dependency('goliath')

end

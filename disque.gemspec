# encoding: utf-8

Gem::Specification.new do |s|
  s.name              = "disque"
  s.version           = "0.0.1"
  s.summary           = "Client for Disque"
  s.description       = "Client for Disque"
  s.authors           = ["Michel Martens"]
  s.email             = ["michel@soveran.com"]
  s.homepage          = "https://github.com/soveran/disque.rb"
  s.files             = `git ls-files`.split("\n")
  s.license           = "MIT"

  s.add_dependency "redic"
end

# encoding: utf-8

Gem::Specification.new do |s|
  s.name              = "disque"
  s.version           = "0.0.2"
  s.summary           = "Client for Disque"
  s.description       = "Disque for Ruby"
  s.authors           = ["Michel Martens", "Damian Janowski"]
  s.email             = ["michel@soveran.com", "damian.janowski@gmail.com"]
  s.homepage          = "https://github.com/soveran/disque-rb"
  s.files             = `git ls-files`.split("\n")
  s.license           = "MIT"

  s.add_dependency "redic"
end

disque-rb
=========

Client for Disque, an in-memory, distributed job queue.

Usage
-----

Create a new Disque client by passing a list of nodes:

```ruby
client = Disque.new(["127.0.0.1:7711", "127.0.0.1:7712", "127.0.0.1:7713"])
```

Now you can add jobs:

```ruby
client.push("foo", "bar", 100)
```

It will push the job "bar" to the queue "foo" with a timeout of 100
ms, and return the id of the job if it was received and replicated
in time.

Then, your workers will do something like this:

```ruby
loop do
  client.fetch(from: ["foo"]) do |job|
    # Do something with `job`
  end
end
```

Installation
------------

You can install it using rubygems.

```
$ gem install disque
```

require_relative "../lib/disque"
require "stringio"

module Silencer
  @output = nil

  def self.start
    $olderr = $stderr
    $stderr = StringIO.new
  end

  def self.stop
    @output = $stderr.string
    $stderr = $olderr
  end

  def self.output
    @output
  end
end

DISQUE_NODES = [
  "127.0.0.1:7710",
  "127.0.0.1:7711",
  "127.0.0.1:7712",
  "127.0.0.1:7713",
]

DISQUE_BAD_NODES  = DISQUE_NODES[0,1]
DISQUE_GOOD_NODES = DISQUE_NODES[1,3]

test "raise if connection is not possible" do
  Silencer.start
  assert_raise(ArgumentError) do
    c = Disque.new(DISQUE_BAD_NODES)
  end
  Silencer.stop

  assert_equal "#<Errno::ECONNREFUSED: Can't connect to: disque://127.0.0.1:7710>\n", Silencer.output
end

test "retry until a connection is reached" do
  Silencer.start
  c = Disque.new(DISQUE_NODES)
  Silencer.stop

  assert_equal "#<Errno::ECONNREFUSED: Can't connect to: disque://127.0.0.1:7710>\n", Silencer.output
  assert_equal "PONG", c.call("PING")
end

test "lack of jobs" do
  c = Disque.new(DISQUE_GOOD_NODES)
  reached = false

  c.fetch(from: ["foo"], timeout: 1) do |job|
    reached = true
  end

  assert_equal false, reached
end

test "one job" do
  c = Disque.new(DISQUE_GOOD_NODES)

  c.push("foo", "bar", 1000)

  c.fetch(from: ["foo"], count: 10) do |job, queue|
    assert_equal "bar", job
  end
end

test "multiple jobs" do
  c = Disque.new(DISQUE_GOOD_NODES)

  c.push("foo", "bar", 1000)
  c.push("foo", "baz", 1000)

  jobs = ["bar", "baz"]

  c.fetch(from: ["foo"], count: 10) do |job, queue|
    assert_equal jobs.pop, job
    assert_equal "foo", queue
  end

  assert jobs.empty?
end

test "multiple queues" do
  c = Disque.new(DISQUE_GOOD_NODES)

  c.push("foo", "bar", 1000)
  c.push("qux", "baz", 1000)

  queues = ["qux", "foo"]
  jobs = ["baz", "bar"]

  result = c.fetch(from: ["foo", "qux"], count: 10) do |job, queue|
    assert_equal jobs.pop, job
    assert_equal queues.pop, queue
  end

  assert jobs.empty?
  assert queues.empty?
end

test "connect to the best node" do
  c1 = Disque.new([DISQUE_GOOD_NODES[0]], cycle: 2)
  c2 = Disque.new([DISQUE_GOOD_NODES[1]], cycle: 2)

  assert c1.prefix != c2.prefix

  # Tamper stats to trigger a reconnection
  c1.stats[c2.prefix] = 10

  c1.push("q1", "j1", 1000)
  c1.push("q1", "j2", 1000)

  c2.push("q1", "j3", 1000)

  c1.fetch(from: ["q1"])
  c1.fetch(from: ["q1"])
  c1.fetch(from: ["q1"])

  # Client should have reconnected
  assert c1.prefix == c2.prefix
end

test "recover after node disconnection" do
  c1 = Disque.new([DISQUE_GOOD_NODES[0]], cycle: 2)

  prefix = c1.prefix

  # Tamper stats to trigger a reconnection to a bad node
  c1.stats["fake"] = 10
  c1.nodes["fake"] = DISQUE_BAD_NODES[0]

  # Delete the other nodes just in case
  c1.nodes.delete_if do |key, val|
    key != prefix &&
    key != "fake"
  end

  c1.push("q1", "j1", 1000)
  c1.push("q1", "j2", 1000)
  c1.push("q1", "j3", 1000)

  c1.fetch(from: ["q1"])
  c1.fetch(from: ["q1"])
  c1.fetch(from: ["q1"])

  # Prefix should stay the same
  assert prefix == c1.prefix
end

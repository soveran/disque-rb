require "redic"

class Disque
  ECONN = [
    Errno::ECONNREFUSED,
    Errno::EINVAL,
  ]

  attr :stats
  attr :nodes
  attr :prefix

  # Create a new Disque client by passing a list of nodes.
  #
  #   Disque.new(["127.0.0.1:7711", "127.0.0.1:7712", "127.0.0.1:7713"])
  #
  # Alternatively, you can pass a single string with a comma-separated
  # list of nodes:
  #
  #   Disque.new("127.0.0.1:7711,127.0.0.1:7712,127.0.0.1:7713")
  #
  # For each operation, a counter is updated to signal which node was
  # the originator of the message. Based on that information, after
  # a full cycle (1000 operations, but configurable on initialization)
  # the stats are checked to see what is the most convenient node
  # to connect to in order to avoid extra jumps.
  #
  # TODO Account for timeout
  def initialize(hosts, auth: nil, cycle: 1000)

    # Split a string of hosts if necessary
    if String === hosts
      hosts = hosts.split(",")
    end

    # Cluster password
    @auth = auth

    # Cycle length
    @cycle = cycle

    # Operations counter
    @count = 0

    @hosts = hosts

    # Known nodes
    @nodes = Hash.new

    # Connection stats
    @stats = Hash.new(0)

    # Main client
    @client = Redic.new

    # Scout client
    @scout = Redic.new

    # Preferred client prefix
    @prefix = nil

    explore!
  end

  def url(host)
    if @auth
      sprintf("disque://:%s@%s", @auth, host)
    else
      sprintf("disque://%s", host)
    end
  end

  # Collect the list of nodes and keep a connection to the
  # node that provided that information.
  def explore!

    # Reset nodes
    @nodes.clear

    @hosts.each do |host|
      begin
        @scout.configure(url(host))

        result = @scout.call!("HELLO")

        # For keeping track of nodes and stats, we use only the
        # first eight characters of the node_id. That's because
        # those eight characters are part of the job_ids, and
        # our stats are based on that.
        @prefix = result[1][0,8]

        # Populate cache
        @nodes[@prefix] = host

        # Connect the main client to the last scouted node
        @client.configure(@scout.url)

        @scout.quit

      rescue *ECONN
        $stderr.puts($!.inspect)
      end
    end

    if @nodes.empty?
      raise ArgumentError, "nodes unavailable"
    end
  end

  def pick_client!
    if @count == @cycle
      @count = 0
      prefix, _ = @stats.max { |a, b| a[1] <=> b[1] }

      if prefix != @prefix
        host = @nodes[prefix]

        if host

          # Reconfigure main client
          @client.configure(url(host))

          # Save current node prefix
          @prefix = prefix

          # Reset stats for this new connection
          @stats.clear
        end
      end
    end
  end

  # Run commands on the active connection. If the
  # connection is lost, new connections are tried
  # until all nodes become unavailable.
  def call(*args)
    @client.call!(*args)
  rescue *ECONN
    explore!
    retry
  end

  # Disque's ADDJOB signature is as follows:
  #
  #     ADDJOB queue_name job <ms-timeout>
  #       [REPLICATE <count>]
  #       [DELAY <sec>]
  #       [RETRY <sec>]
  #       [TTL <sec>]
  #       [MAXLEN <count>]
  #       [ASYNC]
  #
  # You can pass any optional arguments as a hash,
  # for example:
  #
  #     disque.push("foo", "myjob", 1000, ttl: 1, async: true)
  #
  # Note that `async` is a special case because it's just a
  # flag. That's why `true` must be passed as its value.
  def push(queue_name, job, ms_timeout, options = {})
    command = ["ADDJOB", queue_name, job, ms_timeout]
    command += options_to_arguments(options)

    call(*command)
  end

  def fetch(from: [], count: 1, timeout: 0)
    pick_client!

    jobs = call(
      "GETJOB",
        "TIMEOUT", timeout,
        "COUNT", count,
        "FROM", *from)

    if jobs then
      @count += 1

      jobs.each do |queue, msgid, job|

        # Update stats
        @stats[msgid[2,8]] += 1

        if block_given?

          # Process job
          yield(job, queue)

          # Remove job
          call("ACKJOB", msgid)
        end
      end
    end

    return jobs
  end

  def options_to_arguments(options)
    arguments = []

    options.each do |key, value|
      if value == true
        arguments.push(key)
      else
        arguments.push(key, value)
      end
    end

    return arguments
  end

  def quit
    @client.quit
  end
end

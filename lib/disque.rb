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
  # For each operation, a counter is updated to signal which node was
  # the originator of the message. Based on that information, after
  # a full cycle (1000 operations, but configurable on initialization)
  # the stats are checked to see what is the most convenient node
  # to connect to in order to avoid extra jumps.
  #
  # TODO Account for authentication
  # TODO Account for timeout
  def initialize(hosts, cycle: 1000)

    # Cycle length
    @cycle = cycle

    # Operations counter
    @count = 0

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

    explore!(hosts)
  end

  def url(host)
    sprintf("disque://%s", host)
  end

  # Collect the list of nodes by means of `CLUSTER NODES` and
  # keep a connection to the node that provided that information.
  def explore!(hosts)

    # Reset nodes
    @nodes.clear

    hosts.each do |host|
      begin
        @scout.configure(url(host))

        @scout.call("CLUSTER", "NODES").lines do |line|
          id, host, flag = line.split

          prefix = id[0,8]

          if flag == "myself"

            # Configure main client
            @client.configure(@scout.url)

            # Keep track of selected node
            @prefix = prefix
          end

          @nodes[prefix] = host
        end

        break

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
    @client.call(*args)
  rescue *ECONN
    explore!(@nodes.values)
    retry
  end

  # TODO Complete signature with REPLY, DELAY, etc.
  # TODO Determine if stats should be used for ADDJOB.
  def push(queue_name, job, ms_timeout)
    call("ADDJOB", queue_name, job, ms_timeout)
  end

  def fetch(from: [], count: 1, timeout: 0)
    pick_client!

    jobs = call(
      "GETJOBS",
        "TIMEOUT", timeout,
        "COUNT", count,
        "FROM", *from)

    if jobs then
      @count += 1

      jobs.each do |queue, msgid, job|

        # Update stats
        @stats[msgid[2,8]] += 1

        # Process job
        yield(job, queue) if block_given?

        # Remove job
        call("ACKJOB", msgid)
      end
    end

    return jobs
  end
end

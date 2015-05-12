DIR?=./tmp

all: start test stop

test:
	RUBYLIB=./lib cutest tests/*.rb

default:
	@echo make \<port\>
	@echo make \[start\|meet\|stop\|list\]

start: 7711 7712 7713
	@disque -p 7712 CLUSTER MEET 127.0.0.1 7711 > /dev/null
	@disque -p 7713 CLUSTER MEET 127.0.0.1 7712 > /dev/null

stop:
	@kill `cat $(DIR)/disque.*.pid`

%:
	@disque-server \
		--port $@ \
		--dir $(DIR) \
		--daemonize yes \
		--bind 127.0.0.1 \
		--loglevel notice \
		--requirepass test \
		--pidfile disque.$@.pid \
		--appendfilename disque.$@.aof \
		--cluster-config-file disque.$@.nodes \
		--logfile disque.$@.log

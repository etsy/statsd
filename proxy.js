/*jshint node:true, laxcomma:true */

var dgram    = require('dgram')
  , net      = require('net')
  , events   = require('events')
  , logger = require('./lib/logger')
  , hashring = require('hashring')
  , cluster = require('cluster')
  , helpers = require('./lib/helpers')
  , mgmt_server = require('./lib/mgmt_server')
  , configlib   = require('./lib/config');

var packet   = new events.EventEmitter();
var startup_time = Math.round(new Date().getTime() / 1000);
var node_status = {};
var workers = [];  // Keep track of all forked childs
var node_ring = {};
var servers_loaded;
var config;
var l;  // logger

configlib.configFile(process.argv[2], function (conf, oldConfig) {
  config = conf;
  var udp_version = config.address_ipv6 ? 'udp6' : 'udp4';
  var nodes = config.nodes;
  l = new logger.Logger(config.log || {});

  var forkCount = config.forkCount;
  if (forkCount === 'auto') {
    forkCount = require('os').cpus().length;
  }

  var logPrefix = "[" + process.pid + "] ";
  var log = function(msg, type) {
    l.log(logPrefix + msg, type);
  };

  var healthStatus = configlib.healthStatus || 'up';
  var healthCheckInterval = config.checkInterval || 10000;
  var resetStats = config.resetStats || false;

  var broadcastMsg = function(msg) {
    for (var i = 0; i < workers.length; i++) {
      workers[i].send(msg);
    }
  };

  if (forkCount > 1) {
    if (cluster.isMaster) {
      var worker;
      logPrefix += "[master] ";
      log("forking " + forkCount + " childs", "INFO");

      for (var i = 0; i < forkCount; i++) {
        worker = cluster.fork();
        worker.on('message', broadcastMsg);
      }

      cluster.on('online', function(worker) {
        log('worker ' + worker.process.pid + ' is online', 'INFO');
        workers.push(worker);
      });

      cluster.on('exit', function(worker, code, signal) {
        log('worker ' + worker.process.pid + ' died with exit code:' + code + " restarting", 'ERROR');

        // Remove died worker from the array
        for (var i = 0; i < workers.length; i++) {
          if (workers[i].process.pid == worker.process.pid) {
            workers.splice(i, 1);
          }
        }

        worker = cluster.fork();
        worker.on('message', broadcastMsg);
      });

      return;

    } else {
      process.on('message', function(msg) {
        if (msg.healthStatus) {
          healthStatus = msg.healthStatus;
        }
      });
    }
  }

  //load the node_ring object with the available nodes and a weight of 100
  // weight is currently arbitrary but the same for all
  nodes.forEach(function(element, index, array) {
    node_ring[element.host + ':' + element.port] = 100;
  });

  var ring = new hashring(
    node_ring, 'md5', {
      'max cache size': config.cacheSize || 10000,
      //We don't want duplicate keys sent so replicas set to 0
      'replicas': 0
    });

  if (!servers_loaded) {
    // Do an initial rount of health checks prior to starting up the server
    doHealthChecks();

    // Setup the udp listener
    var server_config = config.server  || './servers/udp';
    var servermod = require(server_config);
    var server = servermod.start(config, function (msg, rinfo) {
      // Convert the raw packet to a string (defaults to UTF8 encoding)
      var packet_data = msg.toString();
      var current_metric
        , bits
        , key;
      // If the packet contains a \n then it contains multiple metrics
      if (packet_data.indexOf("\n") > -1) {
        var metrics;
        metrics = packet_data.split("\n");
        // Loop through the metrics and split on : to get mertric name for hashing
        for (var midx in metrics) {
          current_metric = metrics[midx];
          bits = current_metric.split(':');
          key = bits.shift();
          if (current_metric !== '') {
            var new_msg = new Buffer(current_metric);
            packet.emit('send', key, new_msg);
          }
        }

      } else {
        // metrics needs to be an array to fake it for single metric packets
        current_metric = packet_data;
        bits = current_metric.split(':');
        key = bits.shift();
        if (current_metric !== '') {
          packet.emit('send', key, msg);
        }
      }
    });
    var client = dgram.createSocket(udp_version);
    // Listen for the send message, and process the metric key and msg
    packet.on('send', function(key, msg) {
      // retreives the destination for this key
      var statsd_host = ring.get(key);

      // break the retreived host to pass to the send function
      if (statsd_host === undefined) {
        log('Warning: No backend statsd nodes available!', 'WARNING');
      } else {
        var host_config = statsd_host.split(':');

        // Send the mesg to the backend
        client.send(msg, 0, msg.length, host_config[1], host_config[0]);
      }
    });

    mgmt_server.start(
      config,
      function(cmd, parameters, stream) {
        switch(cmd) {
          case "help":
            stream.write("Commands: config, health, status, quit\n\n");
            break;

          case "config":
            helpers.writeConfig(config, stream);
            break;

          case "health":
            if (parameters.length > 0) {
              var cmdaction = parameters[0].toLowerCase();
              if (cmdaction === 'up') {
                healthStatus = 'up';
                if (forkCount > 0) {
                  // Notify the other forks
                  process.send({ healthStatus: healthStatus });
                }
              } else if (cmdaction === 'down') {
                healthStatus = 'down';
                if (forkCount > 0) {
                  // Notify the other forks
                  process.send({ healthStatus: healthStatus });
                }
              }
            }
            stream.write("health: " + healthStatus + "\n");
            break;

          case "status":
            var now    = Math.round(new Date().getTime() / 1000);
            var uptime = now - startup_time;

            stream.write("uptime: " + uptime + "\n");

            stream.write("nodes: ");
            ring.servers.forEach(function(server, index, array) {
              stream.write(server.string + " ");
            });
            stream.write("\n");
            break;

          case "quit":
            stream.end();
            break;

          default:
            stream.write("ERROR\n");
            break;
        }
      },
      function(err, stream) {
        log("MGMT: Caught " + err + ", Moving on", "WARNING");
      }
    );

    servers_loaded = true;
    log("server is up", "INFO");

    // Set the interval for healthchecks
    setInterval(doHealthChecks, healthCheckInterval);
  }

  // Perform health check on all nodes
  function doHealthChecks() {
    nodes.forEach(function(element, index, array) {
      healthcheck(element);
    });
  }

  function markNodeAsHealthy(node_id) {
    if (node_status[node_id] !== undefined) {
      if (node_status[node_id] > 0) {
        var new_server = {};
        new_server[node_id] = 100;
        log('Adding node ' + node_id + ' to the ring.', 'WARNING');
        ring.add(new_server);
        if (resetStats) {
          nodesResetStats();
        }
      }
    }

    node_status[node_id] = 0;
  }

  function markNodeAsUnhealthy(node_id) {
    if (node_status[node_id] === undefined) {
      node_status[node_id] = 1;
    } else {
      node_status[node_id]++;
    }
    if (node_status[node_id] < 2) {
      log('Removing node ' + node_id + ' from the ring.', 'WARNING');
      ring.remove(node_id);
      if (resetStats) {
        nodesResetStats();
      }
    }
  }

  function nodesResetStats() {
    nodes.forEach(function(node, index, array) {
      log('Resetting stats of node ' + node.host + ':' + node.port);
      ['counters', 'timers', 'gauges'].forEach(function(stats_type, index, array) {
        resetStats(node, stats_type);
      })
    });
  }

  // reset a node's stats
  function resetStats(node, stats_type) {
    var client = net.connect({port: node.adminport, host: node.host},
        function() {
            client.write('del' + stats_type + ' *\r\n')
        }).on('error', (err) => {
        // Handle errors here.
         log(err.toString());
      })
  }

  // Perform health check on node
  function healthcheck(node) {
    var ended = false;
    var node_id = node.host + ':' + node.port;
    var client = net.connect(
      {port: node.adminport, host: node.host},
      function onConnect() {
        if (!ended) {
          client.write('health\r\n');
        }
      }
    );

    client.setTimeout(healthCheckInterval, function() {
      client.end();
      markNodeAsUnhealthy(node_id);
      client.removeAllListeners('data');
      ended = true;
    });

    client.on('data', function(data) {
      if (ended) {
        return;
      }

      var health_status = data.toString();
      client.end();
      ended = true;

      if (health_status.indexOf('up') < 0) {
        markNodeAsUnhealthy(node_id);
      } else {
        markNodeAsHealthy(node_id);
      }
    });

    client.on('error', function(e) {
      if (ended) {
        return;
      }

      if (e.code !== 'ECONNREFUSED' && e.code !== 'EHOSTUNREACH' && e.code !== 'ECONNRESET') {
        log('Error during healthcheck on node ' + node_id + ' with ' + e.code, 'ERROR');
      }

      markNodeAsUnhealthy(node_id);
    });
  }

});

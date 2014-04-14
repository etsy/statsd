/*jshint node:true, laxcomma:true */

var dgram  = require('dgram')
  , util    = require('util')
  , net    = require('net')
  , config = require('./lib/config')
  , helpers = require('./lib/helpers')
  , fs     = require('fs')
  , events = require('events')
  , logger = require('./lib/logger')
  , set = require('./lib/set')
  , pm = require('./lib/process_metrics')
  , process_mgmt = require('./lib/process_mgmt')
  , mgmt = require('./lib/mgmt_console');


// initialize data structures with defaults for statsd stats
var keyCounter = {};
var counters = {};
var timers = {};
var timer_counters = {};
var gauges = {};
var sets = {};
var counter_rates = {};
var timer_data = {};
var pctThreshold = null;
var flushInterval, keyFlushInt, server, mgmtServer;
var startup_time = Math.round(new Date().getTime() / 1000);
var backendEvents = new events.EventEmitter();
var healthStatus = config.healthStatus || 'up';
var old_timestamp = 0;
var timestamp_lag_namespace;

// Load and init the backend from the backends/ directory.
function loadBackend(config, name) {
  var backendmod = require(name);

  if (config.debug) {
    l.log("Loading backend: " + name, 'DEBUG');
  }

  var ret = backendmod.init(startup_time, config, backendEvents, l);
  if (!ret) {
    l.log("Failed to load backend: " + name);
    process.exit(1);
  }
}

// global for conf
var conf;

// Flush metrics to each backend.
function flushMetrics() {
  var time_stamp = Math.round(new Date().getTime() / 1000);
  if (old_timestamp > 0) {
    gauges[timestamp_lag_namespace] = (time_stamp - old_timestamp - (Number(conf.flushInterval)/1000));
  }
  old_timestamp = time_stamp;

  var metrics_hash = {
    counters: counters,
    gauges: gauges,
    timers: timers,
    timer_counters: timer_counters,
    sets: sets,
    counter_rates: counter_rates,
    timer_data: timer_data,
    pctThreshold: pctThreshold,
    histogram: conf.histogram,
    ignoreTimerMetrics: conf.ignoreTimerMetrics
  };

  // After all listeners, reset the stats
  backendEvents.once('flush', function clear_metrics(ts, metrics) {
    // TODO: a lot of this should be moved up into an init/constructor so we don't have to do it every
    // single flushInterval....
    // allows us to flag all of these on with a single config but still override them individually
    conf.deleteIdleStats = conf.deleteIdleStats !== undefined ? conf.deleteIdleStats : false;
    if (conf.deleteIdleStats) {
      conf.deleteCounters = conf.deleteCounters !== undefined ? conf.deleteCounters : true;
      conf.deleteTimers = conf.deleteTimers !== undefined ? conf.deleteTimers : true;
      conf.deleteSets = conf.deleteSets !== undefined ? conf.deleteSets : true;
      conf.deleteGauges = conf.deleteGauges !== undefined ? conf.deleteGauges : true;
    }

    // Clear the counters
    conf.deleteCounters = conf.deleteCounters || false;
    for (var counter_key in metrics.counters) {
      if (conf.deleteCounters) {
        if ((counter_key.indexOf("packets_received") != -1) || (counter_key.indexOf("bad_lines_seen") != -1)) {
          metrics.counters[counter_key] = 0;
        } else {
         delete(metrics.counters[counter_key]);
        }
      } else {
        metrics.counters[counter_key] = 0;
      }
    }

    // Clear the timers
    conf.deleteTimers = conf.deleteTimers || false;
    for (var timer_key in metrics.timers) {
      if (conf.deleteTimers) {
        delete(metrics.timers[timer_key]);
        delete(metrics.timer_counters[timer_key]);
      } else {
        metrics.timers[timer_key] = [];
        metrics.timer_counters[timer_key] = 0;
     }
    }

    // Clear the sets
    conf.deleteSets = conf.deleteSets || false;
    for (var set_key in metrics.sets) {
      if (conf.deleteSets) {
        delete(metrics.sets[set_key]);
      } else {
        metrics.sets[set_key] = new set.Set();
      }
    }

	// normally gauges are not reset.  so if we don't delete them, continue to persist previous value
    conf.deleteGauges = conf.deleteGauges || false;
    if (conf.deleteGauges) {
      for (var gauge_key in metrics.gauges) {
        delete(metrics.gauges[gauge_key]);
      }
    }
  });

  pm.process_metrics(metrics_hash, flushInterval, time_stamp, function emitFlush(metrics) {
    backendEvents.emit('flush', time_stamp, metrics);
  });

}

var stats = {
  messages: {
    last_msg_seen: startup_time,
    bad_lines_seen: 0
  }
};

// Global for the logger
var l;

config.configFile(process.argv[2], function (config, oldConfig) {
  conf = config;

  process_mgmt.init(config);

  l = new logger.Logger(config.log || {});

  // setup config for stats prefix
  prefixStats = config.prefixStats;
  prefixStats = prefixStats !== undefined ? prefixStats : "statsd";
  //setup the names for the stats stored in counters{}
  bad_lines_seen   = prefixStats + ".bad_lines_seen";
  packets_received = prefixStats + ".packets_received";
  timestamp_lag_namespace = prefixStats + ".timestamp_lag";

  //now set to zero so we can increment them
  counters[bad_lines_seen]   = 0;
  counters[packets_received] = 0;

  if (server === undefined) {

    // key counting
    var keyFlushInterval = Number((config.keyFlush && config.keyFlush.interval) || 0);

    var udp_version = config.address_ipv6 ? 'udp6' : 'udp4';
    server = dgram.createSocket(udp_version, function (msg, rinfo) {
      backendEvents.emit('packet', msg, rinfo);
      counters[packets_received]++;
      var packet_data = msg.toString();
      if (packet_data.indexOf("\n") > -1) {
        var metrics = packet_data.split("\n");
      } else {
        var metrics = [ packet_data ] ;
      }

      for (var midx in metrics) {
        if (metrics[midx].length === 0) {
          continue;
        }
        if (config.dumpMessages) {
          l.log(metrics[midx].toString());
        }
        var bits = metrics[midx].toString().split(':');
        var key = bits.shift()
                      .replace(/\s+/g, '_')
                      .replace(/\//g, '-')
                      .replace(/[^a-zA-Z_\-0-9\.]/g, '');

        if (keyFlushInterval > 0) {
          if (! keyCounter[key]) {
            keyCounter[key] = 0;
          }
          keyCounter[key] += 1;
        }

        if (bits.length === 0) {
          bits.push("1");
        }

        for (var i = 0; i < bits.length; i++) {
          var sampleRate = 1;
          var fields = bits[i].split("|");
          if (!helpers.is_valid_packet(fields)) {
              l.log('Bad line: ' + fields + ' in msg "' + metrics[midx] +'"');
              counters[bad_lines_seen]++;
              stats.messages.bad_lines_seen++;
              continue;
          }
          if (fields[2]) {
            sampleRate = Number(fields[2].match(/^@([\d\.]+)/)[1]);
          }

          var metric_type = fields[1].trim();
          if (metric_type === "ms") {
            if (! timers[key]) {
              timers[key] = [];
              timer_counters[key] = 0;
            }
            timers[key].push(Number(fields[0] || 0));
            timer_counters[key] += (1 / sampleRate);
          } else if (metric_type === "g") {
            if (gauges[key] && fields[0].match(/^[-+]/)) {
              gauges[key] += Number(fields[0] || 0);
            } else {
              gauges[key] = Number(fields[0] || 0);
            }
          } else if (metric_type === "s") {
            if (! sets[key]) {
              sets[key] = new set.Set();
            }
            sets[key].insert(fields[0] || '0');
          } else {
            if (! counters[key]) {
              counters[key] = 0;
            }
            counters[key] += Number(fields[0] || 1) * (1 / sampleRate);
          }
        }
      }

      stats.messages.last_msg_seen = Math.round(new Date().getTime() / 1000);
    });

    mgmtServer = net.createServer(function(stream) {
      stream.setEncoding('ascii');

      stream.on('error', function(err) {
        l.log('Caught ' + err +', Moving on');
      });

      stream.on('data', function(data) {
        var cmdline = data.trim().split(" ");
        var cmd = cmdline.shift();

        switch(cmd) {
          case "help":
            stream.write("Commands: stats, counters, timers, gauges, delcounters, deltimers, delgauges, health, quit\n\n");
            break;

          case "health":
            if (cmdline.length > 0) {
              var cmdaction = cmdline[0].toLowerCase();
              if (cmdaction === 'up') {
                healthStatus = 'up';
              } else if (cmdaction === 'down') {
                healthStatus = 'down';
              }
            }
            stream.write("health: " + healthStatus + "\n");
            break;

          case "stats":
            var now    = Math.round(new Date().getTime() / 1000);
            var uptime = now - startup_time;

            stream.write("uptime: " + uptime + "\n");

            var stat_writer = function(group, metric, val) {
              var delta;

              if (metric.match("^last_")) {
                delta = now - val;
              }
              else {
                delta = val;
              }

              stream.write(group + "." + metric + ": " + delta + "\n");
            };

            // Loop through the base stats
            for (var group in stats) {
              for (var metric in stats[group]) {
                stat_writer(group, metric, stats[group][metric]);
              }
            }

            backendEvents.once('status', function(writeCb) {
              stream.write("END\n\n");
            });

            // Let each backend contribute its status
            backendEvents.emit('status', function(err, name, stat, val) {
              if (err) {
                l.log("Failed to read stats for backend " +
                        name + ": " + err);
              } else {
                stat_writer(name, stat, val);
              }
            });

            break;

          case "counters":
            stream.write(util.inspect(counters) + "\n");
            stream.write("END\n\n");
            break;

          case "timers":
            stream.write(util.inspect(timers) + "\n");
            stream.write("END\n\n");
            break;

          case "gauges":
            stream.write(util.inspect(gauges) + "\n");
            stream.write("END\n\n");
            break;

          case "delcounters":
            mgmt.delete_stats(counters, cmdline, stream);
            break;

          case "deltimers":
            mgmt.delete_stats(timers, cmdline, stream);
            break;

          case "delgauges":
            mgmt.delete_stats(gauges, cmdline, stream);
            break;

          case "quit":
            stream.end();
            break;

          default:
            stream.write("ERROR\n");
            break;
        }

      });
    });

    server.bind(config.port || 8125, config.address || undefined);
    mgmtServer.listen(config.mgmt_port || 8126, config.mgmt_address || undefined);

    util.log("server is up");

    pctThreshold = config.percentThreshold || 90;
    if (!Array.isArray(pctThreshold)) {
      pctThreshold = [ pctThreshold ]; // listify percentiles so single values work the same
    }

    flushInterval = Number(config.flushInterval || 10000);
    config.flushInterval = flushInterval;

    if (config.backends) {
      for (var i = 0; i < config.backends.length; i++) {
        loadBackend(config, config.backends[i]);
      }
    } else {
      // The default backend is graphite
      loadBackend(config, './backends/graphite');
    }

    // Setup the flush timer
    var flushInt = setInterval(flushMetrics, flushInterval);

    if (keyFlushInterval > 0) {
      var keyFlushPercent = Number((config.keyFlush && config.keyFlush.percent) || 100);
      var keyFlushLog = config.keyFlush && config.keyFlush.log;

      keyFlushInt = setInterval(function () {
        var sortedKeys = [];

        for (var key in keyCounter) {
          sortedKeys.push([key, keyCounter[key]]);
        }

        sortedKeys.sort(function(a, b) { return b[1] - a[1]; });

        var logMessage = "";
        var timeString = (new Date()) + "";

        // only show the top "keyFlushPercent" keys
        for (var i = 0, e = sortedKeys.length * (keyFlushPercent / 100); i < e; i++) {
          logMessage += timeString + " count=" + sortedKeys[i][1] + " key=" + sortedKeys[i][0] + "\n";
        }

        if (keyFlushLog) {
          var logFile = fs.createWriteStream(keyFlushLog, {flags: 'a+'});
          logFile.write(logMessage);
          logFile.end();
        } else {
          process.stdout.write(logMessage);
        }

        // clear the counter
        keyCounter = {};
      }, keyFlushInterval);
    }
  }
});

process.on('exit', function () {
  flushMetrics();
});

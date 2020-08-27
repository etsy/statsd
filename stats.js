/*jshint node:true, laxcomma:true */

const util = require('util');
const config = require('./lib/config');
const helpers = require('./lib/helpers');
const fs = require('fs');
const events = require('events');
const logger = require('./lib/logger');
const set = require('./lib/set');
const pm = require('./lib/process_metrics');
const process_mgmt = require('./lib/process_mgmt');
const mgmt_server = require('./lib/mgmt_server');
const mgmt = require('./lib/mgmt_console');

// initialize data structures with defaults for statsd stats
let keyCounter = {};
let counters = {};
let timers = {};
let timer_counters = {};
let gauges = {};
let gaugesTTL = {};
let sets = {};
let counter_rates = {};
let timer_data = {};
let pctThreshold = null;
let flushInterval, keyFlushInt, serversLoaded, mgmtServer;
let startup_time = Math.round(new Date().getTime() / 1000);
let backendEvents = new events.EventEmitter();
let healthStatus = config.healthStatus || 'up';
let old_timestamp = 0;
let timestamp_lag_namespace;
let keyNameSanitize = true;

// Load and init the backend from the backends/ directory.
function loadBackend(config, name) {
  const backendmod = require(name);

  if (config.debug) {
    l.log("Loading backend: " + name, 'DEBUG');
  }

  const ret = backendmod.init(startup_time, config, backendEvents, l);
  if (!ret) {
    l.log("Failed to load backend: " + name, "ERROR");
    process.exit(1);
  }
}

// Load and init the server from the servers/ directory.
// The callback mimics the dgram 'message' event parameters (msg, rinfo)
//   msg: the message received by the server. may contain more than one metric
//   rinfo: contains remote address information and message length
//      (attributes are .address, .port, .family, .size - you're welcome)
function startServer(config, name, callback) {
  const servermod = require(name);

  if (config.debug) {
    l.log("Loading server: " + name, 'DEBUG');
  }

  const ret = servermod.start(config, callback);
  if (!ret) {
    l.log("Failed to load server: " + name, "ERROR");
    process.exit(1);
  }
}

// global for conf
let conf;

// Flush metrics to each backend.
function flushMetrics() {
  const time_stamp = Math.round(new Date().getTime() / 1000);
  if (old_timestamp > 0) {
    gauges[timestamp_lag_namespace] = (time_stamp - old_timestamp - (Number(conf.flushInterval)/1000));
  }
  old_timestamp = time_stamp;

  const metrics_hash = {
    counters: counters,
    gauges: gauges,
    timers: timers,
    timer_counters: timer_counters,
    sets: sets,
    counter_rates: counter_rates,
    timer_data: timer_data,
    pctThreshold: pctThreshold,
    histogram: conf.histogram
  };

  // After all listeners, reset the stats
  backendEvents.once('flush', function clear_metrics(ts, metrics) {

    // Clear the counters
    conf.deleteCounters = conf.deleteCounters || false;
    for (const counter_key in metrics.counters) {
      if (conf.deleteCounters) {
        if ((counter_key.indexOf("packets_received") != -1) ||
            (counter_key.indexOf("metrics_received") != -1) ||
            (counter_key.indexOf("bad_lines_seen") != -1)) {
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
    for (const timer_key in metrics.timers) {
      if (conf.deleteTimers) {
        delete(metrics.timers[timer_key]);
        delete(metrics.timer_counters[timer_key]);
      } else {
        metrics.timers[timer_key] = [0];
        metrics.timer_counters[timer_key] = 0;
     }
    }

    // Clear the sets
    conf.deleteSets = conf.deleteSets || false;
    for (const set_key in metrics.sets) {
      if (conf.deleteSets) {
        delete(metrics.sets[set_key]);
      } else {
        metrics.sets[set_key] = new set.Set();
      }
    }

    // Normally gauges are not reset.  so if we don't delete them, continue to persist previous value
    conf.deleteGauges = conf.deleteGauges || false;
    if (conf.deleteGauges) {
      for (const gauge_key in gaugesTTL) {
        gaugesTTL[gauge_key]--;

        // if the gauge has been idle for more than the allowed TTL cycles delete it
        if (gaugesTTL[gauge_key] < 1) {
          delete(metrics.gauges[gauge_key]);
          delete(gaugesTTL[gauge_key]);
        }
      }
    }
  });

  pm.process_metrics(metrics_hash, conf.calculatedTimerMetrics, flushInterval, time_stamp, function emitFlush(metrics) {
    backendEvents.emit('flush', time_stamp, metrics);
  });

  // Performing this setTimeout at the end of this method rather than the beginning
  // helps ensure we adapt to negative clock skew by letting the method's latency
  // introduce a short delay that should more than compensate.
  setTimeout(flushMetrics, getFlushTimeout(flushInterval));
}

const stats = {
  messages: {
    last_msg_seen: startup_time,
    bad_lines_seen: 0
  }
};

function sanitizeKeyName(key) {
  if (keyNameSanitize) {
    return key.replace(/\s+/g, '_')
              .replace(/\//g, '-')
              .replace(/[^a-zA-Z_\-0-9\.;=]/g, '');
  } else {
    return key;
  }
}

function getFlushTimeout(interval) {
  const now = new Date().getTime()
  const deltaTime = now - startup_time * 1000;
  const timeoutAttempt = Math.round(deltaTime / interval) + 1;
  const fixedTimeout = (startup_time * 1000 + timeoutAttempt * interval) - now;

  return fixedTimeout;
}

// Global for the logger
let l;

config.configFile(process.argv[2], function (config) {
  conf = config;

  process_mgmt.init(config);

  l = new logger.Logger(config.log || {});

  // force conf.gaugesMaxTTL to 1 if it not a positive integer > 1
  if (helpers.isInteger(conf.gaugesMaxTTL) && conf.gaugesMaxTTL > 1) {
    conf.gaugesMaxTTL = conf.gaugesMaxTTL;
  } else {
    conf.gaugesMaxTTL = 1;
  }

  // allows us to flag all of these on with a single config but still override them individually
  conf.deleteIdleStats = conf.deleteIdleStats !== undefined ? conf.deleteIdleStats : false;
  if (conf.deleteIdleStats) {
    conf.deleteCounters = conf.deleteCounters !== undefined ? conf.deleteCounters : true;
    conf.deleteTimers = conf.deleteTimers !== undefined ? conf.deleteTimers : true;
    conf.deleteSets = conf.deleteSets !== undefined ? conf.deleteSets : true;
    conf.deleteGauges = conf.deleteGauges !== undefined ? conf.deleteGauges : true;
  }

  // if gauges are not being deleted, clear gaugesTTL counters to save memory
  if (! conf.deleteGauges) {
    gaugesTTL = {}
  }
  // setup config for stats prefix
  let prefixStats = config.prefixStats;
  prefixStats = prefixStats !== undefined ? prefixStats : "statsd";
  //setup the names for the stats stored in counters{}
  bad_lines_seen   = prefixStats + ".bad_lines_seen";
  packets_received = prefixStats + ".packets_received";
  metrics_received = prefixStats + ".metrics_received";
  timestamp_lag_namespace = prefixStats + ".timestamp_lag";

  //now set to zero so we can increment them
  counters[bad_lines_seen]   = 0;
  counters[packets_received] = 0;
  counters[metrics_received] = 0;

  if (config.keyNameSanitize !== undefined) {
    keyNameSanitize = config.keyNameSanitize;
  }
  if (!serversLoaded) {

    // key counting
    const keyFlushInterval = Number((config.keyFlush && config.keyFlush.interval) || 0);

    const handlePacket = function (msg, rinfo) {
      backendEvents.emit('packet', msg, rinfo);
      counters[packets_received]++;
      let metrics;
      const packet_data = msg.toString();
      if (packet_data.indexOf("\n") > -1) {
        metrics = packet_data.split("\n");
      } else {
        metrics = [ packet_data ] ;
      }

      for (const midx in metrics) {
        if (metrics[midx].length === 0) {
          continue;
        }

        counters[metrics_received]++;
        if (config.dumpMessages) {
          l.log(metrics[midx].toString());
        }

        let bits = metrics[midx].toString().split('|#');
        let tags = [];
        if (bits.length > 1 && bits[1].length > 0) {
          tags = bits[1].split(',');
        }
        bits = bits[0].split(':');
        let key = bits.shift();
        if (tags.length > 0) {
          key += ';' + tags.map(function(tag) {
            return tag.replace(';', '_').replace(':', '=');
          }).join(';');
        }
        key = sanitizeKeyName(key);

        if (keyFlushInterval > 0) {
          if (! keyCounter[key]) {
            keyCounter[key] = 0;
          }
          keyCounter[key] += 1;
        }

        if (bits.length === 0) {
          bits.push("1");
        }

        for (let i = 0; i < bits.length; i++) {
          let sampleRate = 1;
          const fields = bits[i].split("|");
          if (!helpers.is_valid_packet(fields)) {
              l.log('Bad line: ' + fields + ' in msg "' + metrics[midx] +'"');
              counters[bad_lines_seen]++;
              stats.messages.bad_lines_seen++;
              continue;
          }
          if (fields[2]) {
            sampleRate = Number(fields[2].match(/^@([\d\.]+)/)[1]);
          }

          const metric_type = fields[1].trim();
          if (metric_type === "ms") {
            if (! timers[key]) {
              timers[key] = [];
              timer_counters[key] = 0;
            }
            timers[key].push(Number(fields[0] || 0));
            timer_counters[key] += (1 / sampleRate);
          } else if (metric_type === "g") {
            // if deleteGauges is true reset the max TTL to its initial value
            if (conf.deleteGauges) {
              gaugesTTL[key] = conf.gaugesMaxTTL;
            }
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
    };

    // If config.servers isn't specified, use the top-level config for backwards-compatibility
    const server_config = config.servers || [config];
    for (let i = 0; i < server_config.length; i++) {
      // The default server is UDP
      const server = server_config[i].server || './servers/udp';
      startServer(server_config[i], server, handlePacket);
    }

    mgmt_server.start(
      config,
      function(cmd, parameters, stream) {
        switch(cmd) {
          case "help":
            stream.write("Commands: stats, counters, timers, gauges, delcounters, deltimers, delgauges, health, config, quit\n\n");
            break;

          case "config":
            helpers.writeConfig(config, stream);
            break;

          case "health":
            if (parameters.length > 0) {
              const cmdaction = parameters[0].toLowerCase();
              if (cmdaction === 'up') {
                healthStatus = 'up';
              } else if (cmdaction === 'down') {
                healthStatus = 'down';
              }
            }
            stream.write("health: " + healthStatus + "\n");
            break;

          case "stats":
            const now    = Math.round(new Date().getTime() / 1000);
            const uptime = now - startup_time;

            stream.write("uptime: " + uptime + "\n");

            const stat_writer = function(group, metric, val) {
              let delta;

              if (metric.match("^last_")) {
                delta = now - val;
              }
              else {
                delta = val;
              }

              stream.write(group + "." + metric + ": " + delta + "\n");
            };

            // Loop through the base stats
            for (const group in stats) {
              for (const metric in stats[group]) {
                stat_writer(group, metric, stats[group][metric]);
              }
            }

            backendEvents.once('status', function() {
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
            mgmt.delete_stats(counters, parameters, stream);
            break;

          case "deltimers":
            mgmt.delete_stats(timers, parameters, stream);
            break;

          case "delgauges":
            mgmt.delete_stats(gauges, parameters, stream);
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
        l.log('MGMT: Caught ' + err +', Moving on', 'WARNING');
      }
    );

    serversLoaded = true;
    util.log("server is up", "INFO");

    pctThreshold = config.percentThreshold || 90;
    if (!Array.isArray(pctThreshold)) {
      pctThreshold = [ pctThreshold ]; // listify percentiles so single values work the same
    }

    flushInterval = Number(config.flushInterval || 10000);
    config.flushInterval = flushInterval;

    if (config.backends) {
      for (let j = 0; j < config.backends.length; j++) {
        loadBackend(config, config.backends[j]);
      }
    } else {
      // The default backend is graphite
      loadBackend(config, './backends/graphite');
    }

    // Setup the flush timer
    const flushInt = setTimeout(flushMetrics, getFlushTimeout(flushInterval));

    if (keyFlushInterval > 0) {
      const keyFlushPercent = Number((config.keyFlush && config.keyFlush.percent) || 100);
      const keyFlushLog = config.keyFlush && config.keyFlush.log;

      keyFlushInt = setInterval(function () {
        const sortedKeys = [];

        for (const key in keyCounter) {
          sortedKeys.push([key, keyCounter[key]]);
        }

        sortedKeys.sort(function(a, b) { return b[1] - a[1]; });

        let logMessage = "";
        const timeString = (new Date()) + "";

        // only show the top "keyFlushPercent" keys
        for (let i = 0, e = sortedKeys.length * (keyFlushPercent / 100); i < e; i++) {
          logMessage += timeString + " count=" + sortedKeys[i][1] + " key=" + sortedKeys[i][0] + "\n";
        }

        if (keyFlushLog) {
          const logFile = fs.createWriteStream(keyFlushLog, {flags: 'a+'});
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

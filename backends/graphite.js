/*jshint node:true, laxcomma:true */

/*
 * Flush stats to graphite (http://graphite.wikidot.com/).
 *
 * To enable this backend, include 'graphite' in the backends
 * configuration array:
 *
 *   backends: ['graphite']
 *
 * This backend supports the following config options:
 *
 *   graphiteHost: Hostname of graphite server.
 *   graphitePort: Port for the graphite text collector. Defaults to 2003.
 *   graphitePicklePort: Port for the graphite pickle collector. Defaults to 2004.
 *   graphiteProtocol: Either 'text' or 'pickle'. Defaults to 'text'.
 *
 * If graphiteHost is not specified, metrics are processed but discarded.
 */

var net = require('net');

// this will be instantiated to the logger
var l;

var debug;
var flushInterval;
var graphiteHost;
var graphitePort;
var graphitePicklePort;
var graphiteProtocol;
var flush_counts;

// prefix configuration
var globalPrefix;
var prefixCounter;
var prefixTimer;
var prefixGauge;
var prefixSet;
var globalSuffix;
var prefixStats;
var globalKeySanitize = true;

// set up namespaces
var legacyNamespace  = true;
var globalNamespace  = [];
var counterNamespace = [];
var timerNamespace   = [];
var gaugesNamespace  = [];
var setsNamespace    = [];

var graphiteStats = {};

var post_stats = function graphite_post_stats(stats) {
  var last_flush = graphiteStats.last_flush || 0;
  var last_exception = graphiteStats.last_exception || 0;
  var flush_time = graphiteStats.flush_time || 0;
  var flush_length = graphiteStats.flush_length || 0;

  if (graphiteHost) {
    try {
      var port = graphiteProtocol == 'pickle' ? graphitePicklePort : graphitePort;
      var graphite = net.createConnection(port, graphiteHost);
      graphite.addListener('error', function(connectionException){
        if (debug) {
          l.log(connectionException);
        }
      });
      graphite.on('connect', function() {
        var ts = Math.round(Date.now() / 1000);
        var namespace = globalNamespace.concat(prefixStats).join(".");
        stats.add(namespace + '.graphiteStats.last_exception' + globalSuffix, last_exception, ts);
        stats.add(namespace + '.graphiteStats.last_flush'     + globalSuffix, last_flush    , ts);
        stats.add(namespace + '.graphiteStats.flush_time'     + globalSuffix, flush_time    , ts);
        stats.add(namespace + '.graphiteStats.flush_length'   + globalSuffix, flush_length  , ts);
        var stats_payload = graphiteProtocol == 'pickle' ? stats.toPickle() : stats.toText();

        // Apply the globalPrefix in legacyNamespace mode.
        if(legacyNamespace && globalPrefix.length > 0)
           statString = globalPrefix + "." + statString.slice(0, -1).replace(/\n/g, "\n" + globalPrefix + ".") + "\n";

        var starttime = Date.now();
        this.write(stats_payload);
        this.end();

        graphiteStats.flush_time = (Date.now() - starttime);
        graphiteStats.flush_length = stats_payload.length;
        graphiteStats.last_flush = Math.round(Date.now() / 1000);
      });
    } catch(e){
      if (debug) {
        l.log(e);
      }
      graphiteStats.last_exception = Math.round(Date.now() / 1000);
    }
  }
};

// Minimally necessary pickle opcodes.
var MARK = '(',
    STOP = '.',
    LONG = 'L',
    STRING = 'S',
    APPEND = 'a',
    LIST = 'l',
    TUPLE = 't';

// A single measurement for sending to graphite.
function Metric(key, value, ts) {
  var m = this;
  this.key = key;
  this.value = value;
  this.ts = ts;

  // return a string representation of this metric appropriate
  // for sending to the graphite collector. does not include
  // a trailing newline.
  this.toText = function() {
    return m.key + " " + m.value + " " + m.ts;
  };

  this.toPickle = function() {
    return MARK + STRING + '\'' + m.key + '\'\n' + MARK + LONG + m.ts + 'L\n' + STRING + '\'' + m.value + '\'\n' + TUPLE + TUPLE + APPEND;
  };
}

// A collection of measurements for sending to graphite.
function Stats() {
  var s = this;
  this.metrics = [];
  this.add = function(key, value, ts) {
    s.metrics.push(new Metric(key, value, ts));
  };

  this.toText = function() {
    return s.metrics.map(function(m) { return m.toText(); }).join('\n') + '\n';
  };

  this.toPickle = function() {
    var body = MARK + LIST + s.metrics.map(function(m) { return m.toPickle(); }).join('') + STOP;

    // The first four bytes of the graphite pickle format
    // contain the length of the rest of the payload.
    // We use Buffer because this is binary data.
    var buf = new Buffer(4 + body.length);

    buf.writeUInt32BE(body.length,0);
    buf.write(body,4);

    return buf;
  };
}

var flush_stats = function graphite_flush(ts, metrics) {
  var starttime = Date.now();
  var numStats = 0;
  var key;
  var timer_data_key;
  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var counter_rates = metrics.counter_rates;
  var timer_data = metrics.timer_data;
  var statsd_metrics = metrics.statsd_metrics;

  // Sanitize key for graphite if not done globally
  function sk(key) {
    if (globalKeySanitize) {
      return key;
    } else {
      return key.replace(/\s+/g, '_')
                .replace(/\//g, '-')
                .replace(/[^a-zA-Z_\-0-9\.;=]/g, '');
    }
  };

  function format(namespace, key) {
    var splitName = key.split(';');
    var keyName = sk(splitName[0]);
    var tags = splitName.length > 1 ? (';' + splitName.slice(1).join(';')) : '';

    return namespace.concat(keyName, [].slice.call(arguments, 2)).join('.') + globalSuffix + tags;
  }

  // Flatten all the different types of metrics into a single
  // collection so we can allow serialization to either the graphite
  // text and pickle formats.
  var stats = new Stats();

  for (key in counters) {
    var value = counters[key];
    var valuePerSecond = counter_rates[key]; // pre-calculated "per second" rate

    if (legacyNamespace === true) {
      stats.add(format(counterNamespace, key), valuePerSecond, ts);
      if (flush_counts) {
        stats.add(format(['stats_counts'], key), value, ts);
      }
    } else {
      stats.add(format(counterNamespace, key, 'rate'), valuePerSecond, ts);
      if (flush_counts) {
        stats.add(format(counterNamespace, key, 'count'), value, ts);
      }
    }

    numStats += 1;
  }

  for (key in timer_data) {
    for (timer_data_key in timer_data[key]) {
      if (typeof(timer_data[key][timer_data_key]) === 'number') {
        stats.add(format(timerNamespace, key, timer_data_key), timer_data[key][timer_data_key], ts);
      } else {
        for (var timer_data_sub_key in timer_data[key][timer_data_key]) {
          if (debug) {
            l.log(timer_data[key][timer_data_key][timer_data_sub_key].toString());
          }
          stats.add(format(timerNamespace, key, timer_data_key, timer_data_sub_key),
                    timer_data[key][timer_data_key][timer_data_sub_key], ts);
        }
      }
    }
    numStats += 1;
  }

  for (key in gauges) {
    stats.add(format(gaugesNamespace, key), gauges[key], ts);
    numStats += 1;
  }

  for (key in sets) {
    stats.add(format(setsNamespace, key, 'count'), sets[key].size(), ts);
    numStats += 1;
  }

  if (legacyNamespace === true) {
    stats.add(prefixStats + '.numStats' + globalSuffix, numStats, ts);
    stats.add('stats.' + prefixStats + '.graphiteStats.calculationtime' + globalSuffix, (Date.now() - starttime), ts);
    for (key in statsd_metrics) {
      stats.add('stats.' + prefixStats + '.' + key + globalSuffix, statsd_metrics[key], ts);
    }
  } else {
    stats.add(format(globalNamespace, prefixStats, 'numStats'), numStats, ts);
    stats.add(format(globalNamespace, prefixStats, 'graphiteStats', 'calculationtime'), (Date.now() - starttime) , ts);
    for (key in statsd_metrics) {
      stats.add(format(globalNamespace, prefixStats, key), statsd_metrics[key], ts);
    }
  }
  post_stats(stats);

  if (debug) {
   l.log("numStats: " + numStats);
  }
};

var backend_status = function graphite_status(writeCb) {
  for (var stat in graphiteStats) {
    writeCb(null, 'graphite', stat, graphiteStats[stat]);
  }
};

exports.init = function graphite_init(startup_time, config, events, logger) {
  debug = config.debug;
  l = logger;
  graphiteHost = config.graphiteHost;
  graphitePort = config.graphitePort || 2003;
  graphitePicklePort = config.graphitePicklePort || 2004;
  graphiteProtocol = config.graphiteProtocol || 'text';
  config.graphite = config.graphite || {};
  globalPrefix    = config.graphite.globalPrefix;
  prefixCounter   = config.graphite.prefixCounter;
  prefixTimer     = config.graphite.prefixTimer;
  prefixGauge     = config.graphite.prefixGauge;
  prefixSet       = config.graphite.prefixSet;
  globalSuffix    = config.graphite.globalSuffix;
  legacyNamespace = config.graphite.legacyNamespace;
  prefixStats     = config.prefixStats;

  // set defaults for prefixes & suffix
  globalPrefix  = globalPrefix !== undefined ? globalPrefix : "stats";
  prefixCounter = prefixCounter !== undefined ? prefixCounter : "counters";
  prefixTimer   = prefixTimer !== undefined ? prefixTimer : "timers";
  prefixGauge   = prefixGauge !== undefined ? prefixGauge : "gauges";
  prefixSet     = prefixSet !== undefined ? prefixSet : "sets";
  prefixStats   = prefixStats !== undefined ? prefixStats : "statsd";
  legacyNamespace = legacyNamespace !== undefined ? legacyNamespace : true;

  // In order to unconditionally add this string, it either needs to be an
  // empty string if it was unset, OR prefixed by a . if it was set.
  globalSuffix  = globalSuffix !== undefined ? '.' + globalSuffix : '';

  if (legacyNamespace === false) {
    if (globalPrefix !== "") {
      globalNamespace.push(globalPrefix);
      counterNamespace.push(globalPrefix);
      timerNamespace.push(globalPrefix);
      gaugesNamespace.push(globalPrefix);
      setsNamespace.push(globalPrefix);
    }

    if (prefixCounter !== "") {
      counterNamespace.push(prefixCounter);
    }
    if (prefixTimer !== "") {
      timerNamespace.push(prefixTimer);
    }
    if (prefixGauge !== "") {
      gaugesNamespace.push(prefixGauge);
    }
    if (prefixSet !== "") {
      setsNamespace.push(prefixSet);
    }
  } else {
      globalNamespace = ['stats'];
      counterNamespace = ['stats'];
      timerNamespace = ['stats', 'timers'];
      gaugesNamespace = ['stats', 'gauges'];
      setsNamespace = ['stats', 'sets'];
  }

  graphiteStats.last_flush = startup_time;
  graphiteStats.last_exception = 0;
  graphiteStats.flush_time = 0;
  graphiteStats.flush_length = 0;

  if (config.keyNameSanitize !== undefined) {
    globalKeySanitize = config.keyNameSanitize;
  }

  flushInterval = config.flushInterval;

  flush_counts = typeof(config.flush_counts) === "undefined" ? true : config.flush_counts;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};

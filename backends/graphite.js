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
 *   graphitePort: Port to contact graphite server at.
 */

var net = require('net'),
    logger = require('../lib/logger');

// this will be instantiated to the logger
var l;

var debug;
var flushInterval;
var graphiteHost;
var graphitePort;

// prefix configuration
var globalPrefix;
var prefixPersecond;
var prefixCounter;
var prefixTimer;
var prefixGauge;
var prefixSet;

// set up namespaces
var legacyNamespace  = true;
var globalNamespace  = [];
var counterNamespace = [];
var timerNamespace   = [];
var gaugesNamespace  = [];
var setsNamespace    = [];

var timerRegEx;

var graphiteStats = {};

var post_stats = function graphite_post_stats(statString) {
  var last_flush = graphiteStats.last_flush || 0;
  var last_exception = graphiteStats.last_exception || 0;
  var flush_time = graphiteStats.flush_time || 0;
  var flush_length = graphiteStats.flush_length || 0;
  if (graphiteHost) {
    try {
      var graphite = net.createConnection(graphitePort, graphiteHost);
      graphite.addListener('error', function(connectionException){
        if (debug) {
          l.log(connectionException);
        }
      });
      graphite.on('connect', function() {
        var ts = Math.round(new Date().getTime() / 1000);
        var ts_suffix = ' ' + ts + "\n";
        var namespace = globalNamespace.concat(prefixStats).join(".");
        statString += namespace + '.graphiteStats.last_exception ' + last_exception + ts_suffix;
        statString += namespace + '.graphiteStats.last_flush '     + last_flush     + ts_suffix;
        statString += namespace + '.graphiteStats.flush_time '     + flush_time     + ts_suffix;
        statString += namespace + '.graphiteStats.flush_length '   + flush_length   + ts_suffix;
        var starttime = Date.now();
        this.write(statString);
        this.end();
        graphiteStats.flush_time = (Date.now() - starttime);
        graphiteStats.flush_length = statString.length;
        graphiteStats.last_flush = Math.round(new Date().getTime() / 1000);
      });
    } catch(e){
      if (debug) {
        l.log(e);
      }
      graphiteStats.last_exception = Math.round(new Date().getTime() / 1000);
    }
  }
};

var flush_stats = function graphite_flush(ts, metrics) {
  var ts_suffix = ' ' + ts + "\n";
  var starttime = Date.now();
  var statString = '';
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

  for (key in counters) {
    var namespace = counterNamespace.concat(key);
    var value = counters[key];
    var valuePerSecond = counter_rates[key]; // pre-calculated "per second" rate

    if (legacyNamespace === true) {
      statString += namespace.join(".")   + ' ' + valuePerSecond + ts_suffix;
      statString += 'stats_counts.' + key + ' ' + value          + ts_suffix;
    } else {
      statString += namespace.concat('rate').join(".")  + ' ' + valuePerSecond + ts_suffix;
      statString += namespace.concat('count').join(".") + ' ' + value          + ts_suffix;
    }

    numStats += 1;
  }

  for (key in timer_data) {
    var namespace = timerNamespace.concat(key);
    var the_key = namespace.join(".");
    for (timer_data_key in timer_data[key]) {
      if (!timerRegEx.test(timer_data_key)) {
        continue;
      }
      if (typeof(timer_data[key][timer_data_key]) === 'number') {
        statString += the_key + '.' + timer_data_key + ' ' + timer_data[key][timer_data_key] + ts_suffix;
      } else {
        for (var timer_data_sub_key in timer_data[key][timer_data_key]) {
          if (debug) {
            l.log(timer_data[key][timer_data_key][timer_data_sub_key].toString());
          }
          statString += the_key + '.' + timer_data_key + '.' + timer_data_sub_key + ' ' +
                        timer_data[key][timer_data_key][timer_data_sub_key] + ts_suffix;
        }
      }
    }
    numStats += 1;
  }

  for (key in gauges) {
    var namespace = gaugesNamespace.concat(key);
    statString += namespace.join(".") + ' ' + gauges[key] + ts_suffix;
    numStats += 1;
  }

  for (key in sets) {
    var namespace = setsNamespace.concat(key);
    statString += namespace.join(".") + '.count ' + sets[key].values().length + ts_suffix;
    numStats += 1;
  }

  var namespace = globalNamespace.concat(prefixStats);
  if (legacyNamespace === true) {
    statString += prefixStats + '.numStats ' + numStats + ts_suffix;
    statString += 'stats.' + prefixStats + '.graphiteStats.calculationtime ' + (Date.now() - starttime) + ts_suffix;
    for (key in statsd_metrics) {
      statString += 'stats.' + prefixStats + '.' + key + ' ' + statsd_metrics[key] + ts_suffix;
    }
  } else {
    statString += namespace.join(".") + '.numStats ' + numStats + ts_suffix;
    statString += namespace.join(".") + '.graphiteStats.calculationtime ' + (Date.now() - starttime) + ts_suffix;
    for (key in statsd_metrics) {
      var the_key = namespace.concat(key);
      statString += the_key.join(".") + ' ' + statsd_metrics[key] + ts_suffix;
    }
  }
  post_stats(statString);
  if (debug) {
   l.log("numStats: " + numStats);
  }
};

var backend_status = function graphite_status(writeCb) {
  for (var stat in graphiteStats) {
    writeCb(null, 'graphite', stat, graphiteStats[stat]);
  }
};

exports.init = function graphite_init(startup_time, config, events) {
  l = new logger.Logger(config.log || {});
  debug = config.debug;
  graphiteHost = config.graphiteHost;
  graphitePort = config.graphitePort;
  config.graphite = config.graphite || {};
  globalPrefix    = config.graphite.globalPrefix;
  prefixCounter   = config.graphite.prefixCounter;
  prefixTimer     = config.graphite.prefixTimer;
  prefixGauge     = config.graphite.prefixGauge;
  prefixSet       = config.graphite.prefixSet;
  legacyNamespace = config.graphite.legacyNamespace;
  timerRegEx      = config.graphite.timerRegEx;

  // set defaults for prefixes
  globalPrefix  = globalPrefix !== undefined ? globalPrefix : "stats";
  prefixCounter = prefixCounter !== undefined ? prefixCounter : "counters";
  prefixTimer   = prefixTimer !== undefined ? prefixTimer : "timers";
  prefixGauge   = prefixGauge !== undefined ? prefixGauge : "gauges";
  prefixSet     = prefixSet !== undefined ? prefixSet : "sets";
  legacyNamespace = legacyNamespace !== undefined ? legacyNamespace : true;

  // initialize regex
  timerRegEx     = timerRegEx !== undefined ? timerRegEx : ".*";
  timerRegEx     = new RegExp(timerRegEx);

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
  graphiteStats.last_exception = startup_time;
  graphiteStats.flush_time = 0;
  graphiteStats.flush_length = 0;

  flushInterval = config.flushInterval;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};

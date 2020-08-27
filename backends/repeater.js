/*jshint node:true, laxcomma:true */

var util = require('util')
  , dgram = require('dgram')
  , logger = require('../lib/logger')
  , Pool = require('generic-pool').Pool
  , net = require('net');


var l;
var debug;
var instance;

function logerror(err) {
  if(err && debug) {
    l.log(err);
  }
}



function UDPRepeaterBackend(startupTime, config, emitter) {
  var self = this;

  this.ignore = [ 'statsd.packets_received', 'statsd.bad_lines_seen', 'statsd.metrics_received', 'statsd.timestamp_lag' ];
  this.config = config.repeater || [];
  this.sock = (config.repeaterProtocol == 'udp6') ?
        dgram.createSocket('udp6') :
        dgram.createSocket('udp4');

  // Attach DNS error handler
  this.sock.on('error', function (err) {
    if (debug) {
      l.log('Repeater error: ' + err);
    }
  });

  // attach
  emitter.on('packet', function(packet, rinfo) { self.process(packet, rinfo); });
  emitter.on('flush', function(time_stamp, metrics) { self.flush(time_stamp, metrics); });
}


UDPRepeaterBackend.prototype.process = function(packet, rinfo) {
  var self = this;
  var hosts = self.config;
  for(var i=0; i<hosts.length; i++) {
    if(hosts[i].on != 'flush')
      self.sock.send(packet,0,packet.length,hosts[i].port,hosts[i].host,logerror);
  }
};

/*
  Hosts configured with "on": "flush" will only have packets sent to them
  on the configured flush interval. This can save network bandwidth.
*/
UDPRepeaterBackend.prototype.flush = function(time_stamp, metrics) {
  var self = this;
  var type, packet, stat;
  hosts = self.config;

  // Currently only counters and gauges are repeated via flush.
  var statistics = {
    counters : { data: metrics.counters, suffix: 'c' },
    gauges   : { data: metrics.gauges,   suffix: 'g' }
  };

  for(var i=0; i<hosts.length; i++) {
    if(hosts[i].on != 'flush') continue;

    for(type in statistics) {
      stat = statistics[type];
      for(metric in stat.data) {
        if(self.ignore.indexOf(metric) >= 0) continue;
        if(stat.data[metric] == 0) continue;

        packet = new Buffer(metric + ":" + stat.data[metric] + "|" + stat.suffix);
        self.sock.send(packet,0,packet.length,hosts[i].port,hosts[i].host,logerror);
      }
    }
  }
};

UDPRepeaterBackend.prototype.stop = function(cb) {
  this.sock.close();
  cb();
};



var TCPRepeaterBackend = function(startupTime, config, emitter) {  
  this.config = config;
  this.pools = [];

  var targets = this.config.repeater || [];
  for(var i = 0; i < targets.length; i++) {
    this.pools.push(this.createPool(targets[i]));
  }

  var self = this;
  emitter.on('packet', function(packet, rinfo) { self.process(packet, rinfo); });
};


TCPRepeaterBackend.prototype.createPool = function(server) {  
  return Pool({
    name: server.host + ':' + server.port,

    create: function(cb) {
      var client = net.connect(server.port, server.host);

      function connectError(err) { cb(err, null); }

      client.on('connect', function() {
        client.removeListener('error', connectError);
        cb(null, client);
      });
      
      client.on('error', connectError);
    },

    destroy: function(client) {
      client.end();
    },

    max: 5
  });
};

TCPRepeaterBackend.prototype.process = function(packet, rinfo) {
  function send(buf, pool) {
    pool.acquire(function(err, client) {
      if(err) {
        logerror(err);
      } else {
        client.write(buf, function() {
          pool.release(client);
        });
      }
    });
  }

  for(var i = 0; i < this.pools.length; i++) {
    send(new Buffer(packet.toString() + "\n"), this.pools[i]);
  }
};


TCPRepeaterBackend.prototype.stop = function(cb) {
  var self = this;
  function drain_pool(i) {
    if(i == self.pools.length) {
      cb();
      return;
    }

    self.pools[i].drain(function() {
      self.pools[i].destroyAllNow(function() {
        drain_pool(i + 1);
      });
    });
  }
  
  drain_pool(0);
};


exports.init = function(startupTime, config, emitter, logger) {
  debug = config.debug;
  l = logger;

  var proto = config.repeaterProtocol;
  if(proto == 'tcp') {
    instance = new TCPRepeaterBackend(startupTime, config, emitter);
  } else {
    instance = new UDPRepeaterBackend(startupTime, config, emitter);
  }

  return true;
};


exports.stop = function(cb) {
  if(instance) {
    instance.stop(cb);
    instance = null;
  }
};

var fs           = require('fs'),
    net          = require('net'),
    temp         = require('temp'),
    spawn        = require('child_process').spawn,
    util          = require('util'),
    urlparse     = require('url').parse,
    _            = require('underscore'),
    dgram        = require('dgram'),
    qsparse      = require('querystring').parse,
    http         = require('http');


var writeconfig = function(text,worker,cb,obj){
  temp.open({suffix: '-statsdconf.js'}, function(err, info) {
    if (err) throw err;
    fs.writeSync(info.fd, text);
    fs.close(info.fd, function(err) {
      if (err) throw err;
      worker(info.path,cb,obj);
    });
  });
}

var array_contents_are_equal = function(first,second){
  var intlen = _.intersection(first,second).length;
  var unlen = _.union(first,second).length;
  return (intlen == unlen) && (intlen == first.length);
}

var statsd_send = function(data,sock,host,port,cb){
  send_data = new Buffer(data);
  sock.send(send_data,0,send_data.length,port,host,function(err,bytes){
    if (err) {
      throw err;
    }
    cb();
  });
}

// keep collecting data until a specified timeout period has elapsed
// this will let us capture all data chunks so we don't miss one
var collect_for = function(server,timeout,cb){
  var received = [];
  var in_flight = 0;
  var timed_out = false;
  var collector = function(req,res){
    in_flight += 1;
    var body = '';
    req.on('data',function(data){ body += data; });
    req.on('end',function(){
      received = received.concat(body.split("\n"));
      in_flight -= 1;
      if((in_flight < 1) && timed_out){
          server.removeListener('request',collector);
          cb(received);
      }
    });
  }

  setTimeout(function (){
    timed_out = true;
    if((in_flight < 1)) {
      server.removeListener('connection',collector);
      cb(received);
    }
  },timeout);

  server.on('connection',collector);
}

module.exports = {
  setUp: function (callback) {
    this.testport = 31337;
    this.myflush = 200;
    var configfile = "{graphService: \"graphite\"\n\
               ,  batch: 200 \n\
               ,  flushInterval: " + this.myflush + " \n\
               ,  percentThreshold: 90\n\
               ,  histogram: [ { metric: \"a_test_value\", bins: [1000] } ]\n\
               ,  port: 8125\n\
               ,  dumpMessages: false \n\
               ,  debug: false\n\
               ,  graphite: { legacyNamespace: false }\n\
               ,  graphitePort: " + this.testport + "\n\
               ,  graphiteHost: \"127.0.0.1\"}";

    this.acceptor = net.createServer();
    this.acceptor.listen(this.testport);
    this.sock = dgram.createSocket('udp4');

    this.server_up = true;
    this.ok_to_die = false;
    this.exit_callback_callback = process.exit;

    writeconfig(configfile,function(path,cb,obj){
      obj.path = path;
      obj.server = spawn('node',['stats.js', path]);
      obj.exit_callback = function (code) {
        obj.server_up = false;
        if(!obj.ok_to_die){
          console.log('node server unexpectedly quit with code: ' + code);
          process.exit(1);
        }
        else {
          obj.exit_callback_callback();
        }
      };
      obj.server.on('exit', obj.exit_callback);
      obj.server.stderr.on('data', function (data) {
        console.log('stderr: ' + data.toString().replace(/\n$/,''));
      });
      /*
      obj.server.stdout.on('data', function (data) {
        console.log('stdout: ' + data.toString().replace(/\n$/,''));
      });
      */
      obj.server.stdout.on('data', function (data) {
        // wait until server is up before we finish setUp
        if (data.toString().match(/server is up/)) {
          cb();
        }
      });

    },callback,this);
  },
  tearDown: function (callback) {
    this.sock.close();
    this.acceptor.close();
    this.ok_to_die = true;
    if(this.server_up){
      this.exit_callback_callback = callback;
      this.server.kill();
    } else {
      callback();
    }
  },

  send_well_formed_posts: function (test) {
    test.expect(2);

    // we should integrate a timeout into this
    this.acceptor.once('connection',function(c){
      var body = '';
      c.on('data',function(d){ body += d; });
      c.on('end',function(){
        var rows = body.split("\n");
        var entries = _.map(rows, function(x) {
          var chunks = x.split(' ');
          var data = {};
          data[chunks[0]] = chunks[1];
          return data;
        });
        test.ok(_.include(_.map(entries,function(x) { return _.keys(x)[0] }),'stats.statsd.numStats'),'graphite output includes numStats');
        test.equal(_.find(entries, function(x) { return _.keys(x)[0] == 'stats.statsd.numStats' })['stats.statsd.numStats'],3);
        test.done();
      });
    });
  },

  send_malformed_post: function (test) {
    test.expect(3);

    var testvalue = 1;
    var me = this;
    this.acceptor.once('connection',function(c){
      statsd_send('a_bad_test_value|z',me.sock,'127.0.0.1',8125,function(){
          collect_for(me.acceptor,me.myflush*2,function(strings){
            test.ok(strings.length > 0,'should receive some data');
            var hashes = _.map(strings, function(x) {
              var chunks = x.split(' ');
              var data = {};
              data[chunks[0]] = chunks[1];
              return data;
            });
            var numstat_test = function(post){
              var mykey = 'stats.statsd.numStats';
              return _.include(_.keys(post),mykey) && (post[mykey] == 4);
            };
            test.ok(_.any(hashes,numstat_test), 'statsd.numStats should be 4');

            var bad_lines_seen_value_test = function(post){
              var mykey = 'stats.counters.statsd.bad_lines_seen.count';
              return _.include(_.keys(post),mykey) && (post[mykey] == testvalue);
            };
            test.ok(_.any(hashes,bad_lines_seen_value_test), 'stats.counters.statsd.bad_lines_seen.count should be ' + testvalue);

            test.done();
          });
      });
    });
  },

  timers_are_valid: function (test) {
    test.expect(6);

    var testvalue = 100;
    var me = this;
    this.acceptor.once('connection',function(c){
      statsd_send('a_test_value:' + testvalue + '|ms',me.sock,'127.0.0.1',8125,function(){
          collect_for(me.acceptor,me.myflush*2,function(strings){
            test.ok(strings.length > 0,'should receive some data');
            var hashes = _.map(strings, function(x) {
              var chunks = x.split(' ');
              var data = {};
              data[chunks[0]] = chunks[1];
              return data;
            });
            var numstat_test = function(post){
              var mykey = 'stats.statsd.numStats';
              return _.include(_.keys(post),mykey) && (post[mykey] == 5);
            };
            test.ok(_.any(hashes,numstat_test), 'stats.statsd.numStats should be 5');

            var testtimervalue_test = function(post){
              var mykey = 'stats.timers.a_test_value.mean_90';
              return _.include(_.keys(post),mykey) && (post[mykey] == testvalue);
            };
            var testtimerhistogramvalue_test = function(post){
              var mykey = 'stats.timers.a_test_value.histogram.bin_1000';
              return _.include(_.keys(post),mykey) && (post[mykey] == 1);
            };
            test.ok(_.any(hashes,testtimerhistogramvalue_test), 'stats.timers.a_test_value.histogram.bin_1000 should be ' + 1);
            test.ok(_.any(hashes,testtimervalue_test), 'stats.timers.a_test_value.mean_90 should be ' + testvalue);

            var count_test = function(post, metric){
              var mykey = 'stats.timers.a_test_value.' + metric;
              return _.first(_.filter(_.pluck(post, mykey), function (e) { return e; }));
            };
            test.equals(count_test(hashes, 'count_ps'), 5, 'count_ps should be 5');
            test.equals(count_test(hashes, 'count'), 1, 'count should be 1');

            test.done();
          });
      });
    });
  },

  sampled_timers_are_valid: function (test) {
    test.expect(2);

    var testvalue = 100;
    var me = this;
    this.acceptor.once('connection',function(c){
      statsd_send('a_test_value:' + testvalue + '|ms|@0.1',me.sock,'127.0.0.1',8125,function(){
          collect_for(me.acceptor,me.myflush*2,function(strings){
            var hashes = _.map(strings, function(x) {
              var chunks = x.split(' ');
              var data = {};
              data[chunks[0]] = chunks[1];
              return data;
            });
            var count_test = function(post, metric){
              var mykey = 'stats.timers.a_test_value.' + metric;
              return _.first(_.filter(_.pluck(post, mykey), function (e) { return e }));
            };
            test.equals(count_test(hashes, 'count_ps'), 50, 'count_ps should be 50');
            test.equals(count_test(hashes, 'count'), 10, 'count should be 10');
            test.done();
          });
      });
    });
  },

  counts_are_valid: function (test) {
    test.expect(4);

    var testvalue = 100;
    var me = this;
    this.acceptor.once('connection',function(c){
      statsd_send('a_test_value:' + testvalue + '|c',me.sock,'127.0.0.1',8125,function(){
          collect_for(me.acceptor,me.myflush*2,function(strings){
            test.ok(strings.length > 0,'should receive some data');
            var hashes = _.map(strings, function(x) {
              var chunks = x.split(' ');
              var data = {};
              data[chunks[0]] = chunks[1];
              return data;
            });
            var numstat_test = function(post){
              var mykey = 'stats.statsd.numStats';
              return _.include(_.keys(post),mykey) && (post[mykey] == 5);
            };
            test.ok(_.any(hashes,numstat_test), 'statsd.numStats should be 5');

            var testavgvalue_test = function(post){
              var mykey = 'stats.counters.a_test_value.rate';
              return _.include(_.keys(post),mykey) && (post[mykey] == (testvalue/(me.myflush / 1000)));
            };
            test.ok(_.any(hashes,testavgvalue_test), 'a_test_value.rate should be ' + (testvalue/(me.myflush / 1000)));

            var testcountvalue_test = function(post){
              var mykey = 'stats.counters.a_test_value.count';
              return _.include(_.keys(post),mykey) && (post[mykey] == testvalue);
            };
            test.ok(_.any(hashes,testcountvalue_test), 'a_test_value.count should be ' + testvalue);

            test.done();
          });
      });
    });
  },

  gauges_are_valid: function(test) {
    test.expect(2);

    var testvalue = 70;
    var me = this;
    this.acceptor.once('connection', function(c) {
      statsd_send('a_test_value:' + testvalue + '|g', me.sock, '127.0.0.1', 8125, function() {
        collect_for(me.acceptor, me.myflush*2, function(strings) {
          test.ok(strings.length > 0, 'should receive some data');
          var hashes = _.map(strings, function(x) {
            var chunks = x.split(' ');
            var data = {};
            data[chunks[0]] = chunks[1];
            return data;
          });

          var gaugevalue_test = function(post) {
            var mykey = 'stats.gauges.a_test_value';
            return _.include(_.keys(post), mykey) && (post[mykey] == testvalue);
          };
          test.ok(_.any(hashes, gaugevalue_test), 'stats.gauges.a_test_value should be ' + testvalue);

          test.done();
        });
      });
    });
  },

  gauges_negative_are_valid: function(test) {
    test.expect(2);
    var tempme;
    var testvalue = '(-70)';
    var testresult = Number(testvalue.replace(/\(|\)/g,''));
    var me = this;
    this.acceptor.once('connection', function(c) {
      statsd_send('a_test_value:' + testvalue + '|g', me.sock, '127.0.0.1', 8125, function() {
        collect_for(me.acceptor, me.myflush*2, function(strings) {
          test.ok(strings.length > 0, 'should receive some data');
          var hashes = _.map(strings, function(x) {
            var chunks = x.split(' ');
            var data = {};
            data[chunks[0]] = chunks[1];
            return data;
          });

          var gaugevalue_test = function(post) {
            var mykey = 'stats.gauges.a_test_value';
            tempme = post[mykey];
            return _.include(_.keys(post), mykey) && (post[mykey] == testresult);
          };
          test.ok(_.any(hashes, gaugevalue_test), 'stats.gauges.a_test_value should be ' + tempme);

          test.done();
        });
      });
    });
  },

  gauge_negatives_are_valid: function(test) {
    test.expect(2);

    var teststartvalue = 50;
    var testdeltavalue = '(-3)';
    var testresult = -3;
    var me = this;
    this.acceptor.once('connection', function(c) {
      statsd_send('test_value:' + teststartvalue + '|g', me.sock, '127.0.0.1', 8125, function() {
        statsd_send('test_value:' + testdeltavalue + '|g', me.sock, '127.0.0.1', 8125, function() {
          collect_for(me.acceptor, me.myflush * 2, function(strings) {
            test.ok(strings.length > 0, 'should receive some data');
            var hashes = _.map(strings, function(x) {
              var chunks = x.split(' ');
              var data = {};
              data[chunks[0]] = chunks[1];
              return data;
            });

            var gaugevalue_test = function(post) {
              var mykey = 'stats.gauges.test_value';
              return _.include(_.keys(post), mykey) && (post[mykey] == testresult);
            };
            test.ok(_.any(hashes, gaugevalue_test), 'stats.gauges.test_value should be ' + testresult);

            test.done();
          });
        });
      });
    });
  },

  gauge_modifications_are_valid: function(test) {
    test.expect(2);

    var teststartvalue = 50;
    var testdeltavalue = '-3';
    var testresult = teststartvalue + Number(testdeltavalue);
    var me = this;
    this.acceptor.once('connection', function(c) {
      statsd_send('test_value:' + teststartvalue + '|g', me.sock, '127.0.0.1', 8125, function() {
        statsd_send('test_value:' + testdeltavalue + '|g', me.sock, '127.0.0.1', 8125, function() {
          collect_for(me.acceptor, me.myflush * 2, function(strings) {
            test.ok(strings.length > 0, 'should receive some data');
            var hashes = _.map(strings, function(x) {
              var chunks = x.split(' ');
              var data = {};
              data[chunks[0]] = chunks[1];
              return data;
            });

            var gaugevalue_test = function(post) {
              var mykey = 'stats.gauges.test_value';
              return _.include(_.keys(post), mykey) && (post[mykey] == testresult);
            };
            test.ok(_.any(hashes, gaugevalue_test), 'stats.gauges.test_value should be ' + testresult);

            test.done();
          });
        });
      });
    });
  },

  metric_names_are_sanitized: function(test) {
    var me = this;
    this.acceptor.once('connection', function(c) {
      statsd_send('fo/o:250|c',me.sock,'127.0.0.1',8125,function(){
        statsd_send('b ar:250|c',me.sock,'127.0.0.1',8125,function(){
          statsd_send('foo+bar:250|c',me.sock,'127.0.0.1',8125,function(){
            collect_for(me.acceptor, me.myflush * 2, function(strings){
              var str = strings.join();
              test.ok(str.indexOf('fo-o') !== -1, "Did not map 'fo/o' => 'fo-o'");
              test.ok(str.indexOf('b_ar') !== -1, "Did not map 'b ar' => 'b_ar'");
              test.ok(str.indexOf('foobar') !== -1, "Did not map 'foo+bar' => 'foobar'");
              test.done();
            });
          });
        });
      });
    });
  },

  graphite_tags_are_supported: function(test) {
    var me = this;
    this.acceptor.once('connection', function(c) {
      statsd_send('fo/o;tag1=val1:250|c',me.sock,'127.0.0.1',8125,function(){
        statsd_send('b ar;tag1=val1;tag2=val2:250|c',me.sock,'127.0.0.1',8125,function(){
          statsd_send('foo+bar;tag1=val1;tag3=val3;tag2=val2:250|c',me.sock,'127.0.0.1',8125,function(){
            collect_for(me.acceptor, me.myflush * 2, function(strings){
              var str = strings.join();
              test.ok(str.indexOf('fo-o.count;tag1=val1') !== -1, "Did not map 'fo/o;tag1=val1' => 'fo-o.count;tag1=val1'");
              test.ok(str.indexOf('b_ar.count;tag1=val1;tag2=val2') !== -1, "Did not map 'b ar;tag1=val1;tag2=val2' => 'b_ar.count;tag1=val1;tag2=val2'");
              test.ok(str.indexOf('foobar.count;tag1=val1;tag3=val3;tag2=val2') !== -1, "Did not map 'foo+bar;tag1=val1;tag3=val3;tag2=val2' => 'foobar.count;tag1=val1;tag3=val3;tag2=val2'");
              test.done();
            });
          });
        });
      });
    });
  },

  dogstatsd_tags_are_supported: function(test) {
    var me = this;
    this.acceptor.once('connection', function(c) {
      statsd_send('fo/o:250|c|#tag1:val1',me.sock,'127.0.0.1',8125,function(){
        statsd_send('b ar:250|c|#tag1:val1,tag2:val2',me.sock,'127.0.0.1',8125,function(){
          statsd_send('foo+bar:250|c|#tag1:val;1,tag3:val3,tag2:val2',me.sock,'127.0.0.1',8125,function(){
            collect_for(me.acceptor, me.myflush * 2, function(strings){
              var str = strings.join();
              test.ok(str.indexOf('fo-o.count;tag1=val1') !== -1, "Did not map 'fo/o:250|c|#tag1:val1' => 'fo-o.count;tag1=val1'");
              test.ok(str.indexOf('b_ar.count;tag1=val1;tag2=val2') !== -1, "Did not map 'b ar:250|c|#tag1:val1,tag2:val2' => 'b_ar.count;tag1=val1;tag2=val2'");
              test.ok(str.indexOf('foobar.count;tag1=val_1;tag3=val3;tag2=val2') !== -1, "Did not map 'foo+bar:250|c|#tag1:val;1,tag3:val3,tag2:val2' => 'foobar.count;tag1=val_1;tag3=val3;tag2=val2'");
              test.done();
            });
          });
        });
      });
    });
  }
}

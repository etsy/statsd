const net  = require('net');
const fs = require('fs');

function rinfo(tcpstream, data) {
    this.address = tcpstream.remoteAddress;
    this.port = tcpstream.remotePort;
    this.family = tcpstream.address() ? tcpstream.address().family : 'IPv4';
    this.size = data.length;
}

exports.start = function(config, callback) {
    const server = net.createServer(function(stream) {
      stream.setEncoding('ascii');

      let buffer = '';
      stream.on('data', function(data) {
          buffer += data;
          const offset = buffer.lastIndexOf("\n");
          if (offset > -1) {
             const packet = buffer.slice(0, offset + 1);
             buffer = buffer.slice(offset + 1);
             callback(packet, new rinfo(stream, packet));
          }
      });

      stream.on('error', function (err) {
          // don't kill the server due to connection errors from a client
      });
  });

  server.on('listening', function() {
    config.socket && config.socket_mod && fs.chmod(config.socket, config.socket_mod);
  });

  process.on('exit', function() {
      config.socket && fs.unlinkSync(config.socket);
  });

  server.listen(config.socket || config.port || 8125, config.address || undefined);
  this.server = server;
  return true;
};

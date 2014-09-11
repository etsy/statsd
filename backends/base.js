/*jshint node:true, laxcomma:true */
'use strict';

function BackendBase(startupTime, config, emitter, logger) {
	this.startupTime = startupTime;
	this.config = config;
	this.emitter = emitter;
	this.logger = logger;

	if (this.onFlushEvent) {
		emitter.on('flush', this.onFlushEvent.bind(this));
	}
	if (this.onStatusEvent) {
		emitter.on('status', this.onStatusEvent.bind(this));
	}
	if (this.onPacketEvent) {
		emitter.on('packet', this.onPacketEvent.bind(this));
	}
}

module.exports = BackendBase;
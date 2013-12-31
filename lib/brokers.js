'use strict'

var net = require('net'),
	util = require('util'),
	events = require('events');

var KafkaBrokers = function() {
	this.hosts = [];
}

util.inherits(KafkaBrokers, events.EventEmitter)

KafkaBrokers.prototype.create = function() {
	var broker = {};
	if(arguments.length === 1) {
		var tmp = arguments[0].split(':');
		broker.host = tmp[0];
		broker.port = tmp[1] ? parseInt(tmp[1]) : 9092;
	}
	else {
		broker.host = arguments[0];
		broker.port = parseInt(arguments[1]);
	}
	return broker;
}

KafkaBrokers.prototype.connect = function(broker, index, callback) {
	var self = this;
	if(typeof broker.requested === 'undefined') {
		broker.requested = [];
	}
	if(typeof broker.socket === 'undefined') {
		broker.socket = net.connect({ host: broker.host , port: broker.port }, function() {
			callback && callback();
		})
		broker.socket
			.on('data', function(data) {
				self.emit('data', self.hosts[index], data);
			})
			.on('end', function() {
				delete broker.socket;
			})
			.on('error', function(err) {
				this.emit('end');
			})

	}
	callback && callback();
}

KafkaBrokers.prototype.add = function(arg, callback) {
	var index = this.index(this.hosts, arg);
	var broker = (typeof arg === 'string' ? this.create(arg) : arg);
	if(index < 0) {
		this.hosts.push(broker);
		this.connect(broker, this.hosts.length - 1, function() {
			callback && callback();
		});
	}
	else {
		for(var key in broker) {
			this.hosts[index][key] = broker[key];
		}
		this.connect(this.hosts[index], index, function() {
			callback && callback();
		});
	}
}

KafkaBrokers.prototype.index = function(array, arg) {
	var broker = (typeof arg === 'string' ? this.create(arg) : arg)
	for(var i = 0; i < array.length; i++) {
		if(array[i].host === broker.host && array[i].port === broker.port) {
			return i;
		}
	}
	return -1;
}

KafkaBrokers.prototype.getById = function(id) {
	for(var i = 0; i < this.hosts.length; i++) {
		if(typeof this.hosts[i].nodeId !== 'undefined' && this.hosts[i].nodeId === id) {
			return this.hosts[i];
		}
	}
}

KafkaBrokers.prototype.random = function() {
	return this.hosts[Math.floor(Math.random() * this.hosts.length)];
}

KafkaBrokers.prototype.clean = function(brokers) {
	for(var i = 0; i < this.hosts.length; i++) {
		if(this.index(brokers, this.hosts[i]) < 0) {
			this.hosts.splice(i--, 1);
		}
	}
}

module.exports = KafkaBrokers;
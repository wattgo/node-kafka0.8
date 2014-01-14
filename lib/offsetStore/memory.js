'use strict'

var util = require('util'),
	EventEmitter = require('events').EventEmitter;

var memoryStore = function(options) {
	this.store = {};
}

util.inherits(memoryStore, EventEmitter)

memoryStore.prototype.init = function() {
	this.emit('ready');
}

memoryStore.prototype.fetch = function(topic, group, partition, callback) {
	var key = topic + group + partition;
	callback(typeof this.store[key] !== 'undefined' ? this.store[key] : -1);
}

memoryStore.prototype.commit = function(offset, topic, group, partition, callback) {
	var key = topic + group + partition;
	this.store[key] = offset;
	callback && callback();
}

memoryStore.prototype.cleanup = function() {
	delete this.store;
}

module.exports = memoryStore;
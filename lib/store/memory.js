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

memoryStore.prototype.fetchOffset = function(groupId, topic, partitionId, callback) {
	var key = groupId + topic + partitionId;
	callback(typeof this.store[key] !== 'undefined' ? this.store[key] : -1);
}

memoryStore.prototype.commitOffset = function(groupId, topic, partitionId, offset, callback) {
	var key = groupId + topic + partitionId;
	this.store[key] = offset;
	callback && callback();
}

memoryStore.prototype.cleanup = function() {
	delete this.store;
}

module.exports = memoryStore;
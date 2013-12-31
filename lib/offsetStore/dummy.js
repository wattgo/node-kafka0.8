'use strict'

var util = require('util'),
	EventEmitter = require('events').EventEmitter;

var dummyStore = function(options) {
	this.options = options || {};
}

util.inherits(dummyStore, EventEmitter)

dummyStore.prototype.init = function() {

}

dummyStore.prototype.fetch = function(topic, group, partition, callback) {

}

dummyStore.prototype.commit = function(offset, topic, group, partition, callback) {

}

dummyStore.prototype.cleanup = function() {

}

module.exports = dummyStore;
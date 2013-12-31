'use strict'

var util = require('util'),
	EventEmitter = require('events').EventEmitter;

var zookeeperStore = function(options) {
	this.options = options || {};
}

util.inherits(zookeeperStore, EventEmitter)

zookeeperStore.prototype.init = function() {

}

zookeeperStore.prototype.fetch = function(topic, group, partition, callback) {

}

zookeeperStore.prototype.commit = function(offset, topic, group, partition, callback) {

}

zookeeperStore.prototype.cleanup = function() {

}

module.exports = zookeeperStore;

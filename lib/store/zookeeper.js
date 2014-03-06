'use strict'

var util = require('util'),
	EventEmitter = require('events').EventEmitter,
	zookeeper = require('node-zookeeper-client');

var zookeeperStore = function(options) {
	if(typeof options.zkClient === 'undefined') {
		this.options.connectionString = options.connectionString || 'localhost:2181';
	}
	else {
		this.store = options.zkClient.zk;
	}
}

util.inherits(zookeeperStore, EventEmitter)

zookeeperStore.prototype.init = function() {
	var self = this;
	if(typeof this.store === 'undefined') {
		this.store = zookeeper.createClient(this.options.connectionString);
		this.store.on('connected', function() {
			self.emit('ready');
		})
	}
	else {
		self.emit('ready');
	}
}

/* /consumers/[groupId]/offsets/[topic]/[partitionId] -> long (offset) */

zookeeperStore.prototype.fetchOffset = function(groupId, topic, partitionId, callback) {
	this.store.getData('/consumers/' + groupId + '/offsets/' + topic + '/' + partitionId, function (error, data, stats) {
		if (error || typeof data === 'undefined') {
			callback(-1);
		}
		else {
			var json = JSON.parse(data);
			callback(json.offset);
		}
	})
}

zookeeperStore.prototype.commitOffset = function(groupId, topic, partitionId, offset, callback) {
	var data = {
		version: 1,
		offset: offset
	}
	var buffer = new Buffer(JSON.stringify(data));
	var path = '/consumers/' + groupId + '/offsets/' + topic + '/' + partitionId;
	var self = this;
	this.store.mkdirp(path, function(err, path) {
		self.store.setData(path, buffer, -1, function (error, stat) {
			callback(error, stat);
		})
	})
}

zookeeperStore.prototype.cleanup = function() {

}

module.exports = zookeeperStore;

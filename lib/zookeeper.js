'use strict'

var zookeeper = require('node-zookeeper-client'),
	util = require('util'),
	async = require('async'),
	EventEmitter = require('events').EventEmitter;

var KafkaZookeeper = function (connectionString) {
	var self = this;
	self.startup = true;
	self.zk = zookeeper.createClient(connectionString || 'localhost:2181');
	self.zk.on('connected', function () {
		//console.log('[kafka zookeeper] connected');
		self.getBrokers();
	});
}

util.inherits(KafkaZookeeper, EventEmitter);

KafkaZookeeper.prototype.getBrokers = function () {
	var self = this;
	self.zk.getChildren(
		'/brokers/ids'
		, function () {
			self.getBrokers();
		}
		, function (error, children, stats) {
			if (typeof children !== 'undefined' && children.length) {
				var brokers = [];
				async.forEach(
					children
					, function (brokerID, callback) {
						self.getBrokerInfo(
							brokerID,
							function (err, info) {
								if (!err) {
									brokers.push(JSON.parse(info.toString()));
								}
								callback();
							}
						);
					}
					, function (done) {
						if(self.startup) {
							self.startup = false;
							self.emit('ready', brokers);
						}
						else {
							self.emit('zkBrokersChanged', brokers);
						}
					}
				);
			}
			else {
				if(self.startup) {
					self.startup = false;
					self.emit('ready', []);
				}
				else {
					self.emit('zkBrokersChanged', []);
				}
			}
		}
	);
}

KafkaZookeeper.prototype.getBrokerInfo = function (brokerID, callback) {
	var self = this;
	self.zk.getData(
		'/brokers/ids/' + brokerID
		, function (err, data) {
			if (err) {
				//console.log('[kafka zookeeper] getBrokerInfo', err);
			}
			callback(err, data);
		}
	)
}

module.exports = KafkaZookeeper;

'use strict'

var zookeeper = require('node-zookeeper-client'),
	util = require('util'),
	async = require('async'),
	EventEmitter = require('events').EventEmitter;

var brokersPath = {
	ids: '/brokers/ids',
	topics: '/brokers/topics'
}

var KafkaZookeeper = function (connectionString) {
	var self = this;
	self.startup = true;
	self.zk = zookeeper.createClient(connectionString || 'localhost:2181');
	self.zk.on('connected', function () {
		self.getBrokers();
	});
}

util.inherits(KafkaZookeeper, EventEmitter);

KafkaZookeeper.prototype.getBrokers = function () {
	var self = this;
	self.zk.getChildren(
		brokersPath.ids, 
		function () {
			self.getBrokers();
		},
		function (error, children, stats) {
			if (typeof children !== 'undefined' && children.length) {
				var brokers = [];
				async.forEach(
					children,
					function (brokerID, callback) {
						self.getBrokerInfo(
							brokerID,
							function (error, info) {
								if (!error) {
									brokers.push(JSON.parse(info.toString()));
								}
								callback();
							}
						);
					},
					function (done) {
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
				self.emit('zkBrokersChanged', []);
			}
		}
	);
}

KafkaZookeeper.prototype.getBrokerInfo = function (brokerID, callback) {
	var self = this;
	self.zk.getData(
		brokersPath.ids + '/' + brokerID,
		function (error, data) {
			if (error) {
				console.log('zookeeper: getBrokerInfo:', error);
			}
			callback(error, data);
		}
	)
}

module.exports = KafkaZookeeper;

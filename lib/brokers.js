'use strict'

var net = require('net'),
	util = require('util'),
	events = require('events'),
	dns = require('dns'),
	async = require('async');

var KafkaBrokers = function(options) {
	this.options = options || {};
	this.options.reconnectMs = this.options.reconnectMs || 1000;
	this.options.dnsLookup = this.options.dnsLookup || true;
	this.list = [];
}

util.inherits(KafkaBrokers, events.EventEmitter)

KafkaBrokers.prototype.create = function(metadata, callback) {
	var self = this;
	var broker = {
		connected: false,
		connecting: false,
		host: metadata.host,
		port: metadata.port,
		nodeId: metadata.nodeId,
		topics: (typeof metadata.Topics !== 'undefined' ? metadata.Topics : []),
		requests: [],
		response: {
			buffering: false,
			buffer: new Buffer(0),
			bytesToRead: 0
		}
	}
	if(self.options.dnsLookup) {
		dns.lookup(broker.host, function(err, ip) {
			broker.ip = ip;
			self.add(broker, function(broker) {
				callback && callback(broker);
			})
		});
	}
	else {
		self.add(broker, function(broker) {
			callback && callback(broker);
		})
	}
}

KafkaBrokers.prototype.add = function(broker, callback) {
	var self = this;
	self.exists(broker, function(exists, index) {
		if(!exists) {
			self.list.push(broker);
			self.connect(broker, function(broker) {
				callback && callback(broker);
			});
		}
		else {
			for(var k in broker) {
				if(typeof self.list[index][k] === 'undefined') {
					self.list[index][k] = broker[k];
				}
			}
			callback && callback(broker);
		}
	})
}

KafkaBrokers.prototype.exists = function(metadata, callback) {
	var self = this;
	var index = -1;
	async.eachSeries(self.list, function(current, next) {
		index++;
		if((self.options.dnsLookup && metadata.ip === current.ip && metadata.port === current.port) || (!self.options.dnsLookup && metadata.host === current.host && metadata.port === current.port)) {
			next(true);
		}
		else {
			next();
		}
	}
	, function(found) {
		callback(found, index);
	})
}

KafkaBrokers.prototype.connect = function(broker, callback) {
	var self = this;

	if(!broker.connected && !broker.connecting) {
		console.log('[kafka brokers] connecting to ' + (typeof broker.ip !== 'undefined' ? broker.ip : broker.host) + ':' + broker.port)
		broker.connecting = true;
		broker.socket = net.connect(broker.port, (typeof broker.ip !== 'undefined' ? broker.ip : broker.host), function() {
			broker.socket.setNoDelay(true);
			console.log('[kafka brokers] connected to ' + (typeof broker.ip !== 'undefined' ? broker.ip : broker.host) + ':' + broker.port)
			broker.connected = true;
			broker.connecting = false;
			callback && callback(broker);
		})

		broker.socket
			.on('data', function(data) {
				self.emit('data', broker, data);
			})

			.on('end', function() {
				broker.connected = false;
				broker.connecting = false;
				broker.response = {
					buffering: false,
					buffer: new Buffer(0),
					bytesToRead: 0
				}

				setTimeout(function() {
					console.log('[kafka brokers] reconnecting to ' + (typeof broker.ip !== 'undefined' ? broker.ip : broker.host) + ':' + broker.port)
					self.connect(broker, callback);
				}, self.options.reconnectMs)
			})

			.on('error', function(err) {
				console.log('[kafka brokers]', err)
				this.emit('end');
			})

			.on('timeout', function(err) {
				console.log('[kafka brokers] timeout ' + (typeof broker.ip !== 'undefined' ? broker.ip : broker.host) + ':' + broker.port)
			})
	}
	else {
		callback && callback(broker);
	}
}

KafkaBrokers.prototype.byId = function(nodeId, callback) {
	async.each(this.list, function(broker, next) {
		broker.nodeId === nodeId
			? next(broker)
			: next()
	}
	, function(found) {
		callback(found ? null : new Error('no broker found with id ' + nodeId), found);
	})
}

KafkaBrokers.prototype.byTopic = function(topicName, partitionId, callback) {
	async.each(this.list, function(broker, nextBroker) {
		async.each(broker.topics, function(topic, nextTopic) {
			if(topic.TopicName === topicName) {
				async.each(topic.Partitions, function(partition, nextPartition) {
					partition.Partition === partitionId
						? nextPartition(broker)
						: nextPartition()
				}
				, function(brokerFound) {
					nextTopic(brokerFound);
				})
			}
			else {
				nextTopic();
			}
		}
		, function(brokerFound) {
			nextBroker(brokerFound);
		})
	}
	, function(brokerFound) {
		callback(brokerFound ? null : new Error('no broker found for ' + topicName + ' and partition ' + partitionId), brokerFound);
	})
}

KafkaBrokers.prototype.topicByName = function(broker, name, callback) {
	async.each(broker.topics, function(current, next) {
		current.TopicName === name
			? next(current)
			: next()
	}
	, function(found) {
		callback(found ? null : new Error('topic not found'), found);
	});
}

KafkaBrokers.prototype.hasPartition = function(topic, partitionId, callback) {
	async.each(topic.Partitions, function(current, next) {
		current.Partition === partitionId
			? next(current)
			: next();
	}
	, function(found) {
		callback(found ? true : false, found);
	});
}

KafkaBrokers.prototype.hasTopic = function(broker, topicName, callback) {
	async.each(broker.topics, function(current, next) {
		current.TopicName === topicName
			? next(current)
			: next();
	}
	, function(found) {
		callback(found ? true : false, found);
	});
}

KafkaBrokers.prototype.addTopics = function(broker, topics, callback) {
	var self = this;
	async.each(topics, function(newTopic, nextNewTopic) {
		self.hasTopic(broker, newTopic.TopicName, function(existsT, topic) {
			if(existsT) {
				if(topic.TopicErrorCode) {
					topic = newTopic;
					nextNewTopic();
				}
				else {
					async.each(newTopic.Partitions, function(newPartition, nextNewPartition) {
						self.hasPartition(topic, newPartition.Partition, function(existsP, partition) {
							if(!existsP) {
								topic.Partitions.push(newPartition);
							}
							else if(partition.PartitionErrorCode) {
								partition = newPartition;
							}
							nextNewPartition();
						})
					}
					, function() {
						nextNewTopic();
					});
				}
			}
			else {
				broker.topics.push(newTopic);
				nextNewTopic();
			}
		})
	}
	, function(err) {
		callback(err);
	});
}

KafkaBrokers.prototype.random = function(callback) {
	async.each(this.list, function(broker, next) {
		broker.connected
			? next(broker)
			: next()
	}
	, function(found) {
		callback(found ? null : new Error('no broker found'), found);
	})
}

module.exports = KafkaBrokers;

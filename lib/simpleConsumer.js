'use strict'

var KafkaTransport = require('./transport'),
	async = require('async'),
	Binary = require('binary'),
	util = require('util'),
	Events = require('events');

var KafkaSimpleConsumer = function (options, callback) {
	var self = this;

	this.state = options.transport ? 0 : -1;

	this.transport = options.transport || new KafkaTransport(options)
	this.options = options || {};
	this.options.maxBytes = options.maxBytes || (1024 * 1024);
	this.options.maxWaitTime = options.maxWaitTime || 10 * 3000;
	this.options.minBytes = options.minBytes || 0;
	this.options.clientId = options.clientId || this.transport.defaultClientId;

	if(typeof options.offsetStore === 'undefined') {
		throw new Error('no offset store');
	}

	this.offsetStore = options.offsetStore;
	this.offsetStore.init();

	this.offsetStore.on('ready', function() {
		//console.log('store ready');
		++self.state > 0 && callback && callback();
	})

	this.transport.on('ready', function() {
		++self.state > 0 && callback && callback();
	})
}

util.inherits(KafkaSimpleConsumer, Events.EventEmitter);


/*
 * TODO: handle multiple payloads
 */

KafkaSimpleConsumer.prototype.consume = function(topicName, partition, callback) {
	var self = this;
	var broker;

	if(typeof self.transport.topics !== 'undefined') {
		for(var i = 0; i < self.transport.topics.length; i++) {
			var item = self.transport.topics[i];
			if(item.Topic === topicName) {
				for(var j = 0; j < item.Partitions.length; j++) {
					if(item.Partitions[j].Partition === partition) {
						item.Partitions[j].Leader
						broker = self.transport.brokers.getById(item.Partitions[j].Leader);
					}
				}
				break;
			}
		}

		if(typeof broker !== 'undefined') {
			self.offsetStore.fetch(topicName, self.options.clientId, partition, function(lastOffset) {
				var request = [{
					TopicName: topicName,
					Partitions: [{
						Partition: partition,
						FetchOffset: lastOffset,
						MaxBytes: self.options.maxBytes
					}]
				}]
				self.transport.fetchRequest(broker, self.options.clientId, -1, self.options.maxWaitTime, self.options.minBytes, request, function(response) {
					var currentPartition = response.Topics[0].Partitions[0];
					var highOffset = currentPartition.HighwaterMarkOffset;
					self.transport.decodeMessageSet(currentPartition.MessageSet, function(msg, offset, message) {
						//console.log(offset, lastOffset, highOffset)
						//if(offset >= lastOffset) {
							self.offsetStore.commit(offset, topicName, self.options.clientId, partition, function() {
								callback ? callback(msg, offset) : self.emit('message', msg, offset)
							});
						//}
					}, function() {
						self.offsetStore.commit(highOffset, topicName, self.options.clientId, partition);
					});
				})
			});
		}
		else {

		}
	}

};

module.exports = KafkaSimpleConsumer;

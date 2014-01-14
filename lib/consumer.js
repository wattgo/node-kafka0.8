'use strict'

var KafkaTransport = require('./transport'),
	async = require('async'),
	Binary = require('binary'),
	util = require('util'),
	Events = require('events'),
  offsetMemoryStore = require('./offsetStore/memory');

var KafkaConsumer = function (options, callback) {
	var self = this;

	this.state = options.transport ? 0 : -1;

	this.transport = options.transport || new KafkaTransport(options)
	this.options = options || {};
	this.options.maxBytes = options.maxBytes || (1024 * 1024);
	this.options.maxWaitTime = options.maxWaitTime || 10 * 3000;
	this.options.minBytes = options.minBytes || 0;
	this.options.clientId = options.clientId || this.transport.defaultClientId;

	if(typeof options.offsetStore === 'undefined') {
    	this.offsetStore = new offsetMemoryStore();
	}
	else {
	 	this.offsetStore = options.offsetStore;
	}
	this.offsetStore.init();

	this.offsetStore.on('ready', function() {
		//console.log('store ready');
		++self.state > 0 && callback && callback();
	})

	this.transport.on('ready', function() {
		++self.state > 0 && callback && callback();
	})

}

util.inherits(KafkaConsumer, Events.EventEmitter);


/*
 * TODO:
 * 	- handle multiple payloads
 */

KafkaConsumer.prototype.consume = function(topicName, partition, serializer, callback, endCallback) {
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
						FetchOffset: lastOffset + 1,
						MaxBytes: self.options.maxBytes
					}]
				}]
				self.transport.fetchRequest(broker, self.options.clientId, -1, self.options.maxWaitTime, self.options.minBytes, request, function(response) {
					if(response.Topics.length) {
						var currentPartition = response.Topics[0].Partitions[0];
						var startOffset = lastOffset;
						self.transport.decodeMessageSet(currentPartition.MessageSet, function(buffer, message, nextCallback) {
							var msg;
							// TODO: wrap deserialize in callback before storing offset
							try {
								msg = serializer.deserialize(buffer);
							}
							catch(e) {
								msg = buffer.toString();
							}
							startOffset++;
							self.offsetStore.commit(startOffset, topicName, self.options.clientId, partition, function() {
								callback(msg, startOffset, nextCallback);
							});
						},
						function() {
							endCallback && endCallback();
						})
					}
					else {
						endCallback(new Error('no response'));
					}
				})
			});
		}
	}

};

module.exports = KafkaConsumer;

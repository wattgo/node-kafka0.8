'use strict'

var KafkaTransport = require('./transport'),
	Events = require('events'),
	util = require('util')

var KafkaProducer = function (options, callback) {
	this.transport = options.transport || new KafkaTransport(options);
	this.options = options || {};
	this.options.requiredAcks = options.requiredAcks || 1;
	this.options.timeout = options.timeout || 1000 * 30;
	this.options.clientId = options.clientId || this.transport.defaultClientId;

	this.transport.on('ready', function() {
		callback && callback();
	})
}

util.inherits(KafkaProducer, Events.EventEmitter);

KafkaProducer.prototype.produce = function(payload, callback) {
	var self = this;
	var topics = [];

	var broker;
	var metatopics = this.transport.topics;

	if(typeof metatopics !== 'undefined') {
		// add error check
		for(var i = 0; i < metatopics.length; i++) {
			if(metatopics[i].Topic === payload.topic) {
				for(var j = 0; j < metatopics[i].Partitions.length; j++) {
					if(metatopics[i].Partitions[j].Partition === payload.partition) {
						broker = this.transport.brokers.getById(metatopics[i].Partitions[j].Leader);
						break;
					}
				}
				break;
			}
		}

		if(typeof broker !== 'undefined') {
			this.transport.encodeMessageSet(payload.messages, self.transport.compressions.none, function(mSet) {
				var topic = {
					TopicName: payload.topic,
					Partitions: [ {	Partition: payload.partition, MessageSet: mSet } ]
				}
				topics.push(topic);

				self.transport.produceRequest(broker, self.options.clientId, self.options.requiredAcks, self.options.timeout, topics, function(response) {
					callback(null, response);
				})
			})
		}
		else {
			var error = new Error('no broker found for ' + payload.topic)
			callback(error, {});
		}
	}
}

module.exports = KafkaProducer;
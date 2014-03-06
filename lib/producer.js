'use strict'

var KafkaTransport = require('./transport'),
	Events = require('events'),
	util = require('util')

var KafkaProducer = function (options, callback) {
	this.options = options || {};
	this.options.requiredAcks = options.requiredAcks || 1
	this.options.produceTimeout = 1000 * 30;
    this.transport = options.transport || new KafkaTransport(this.options);
	this.options.clientId = options.clientId || this.transport.clientId;
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
			if(metatopics[i].TopicName === payload.topic) {
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
			var szSet = [];
			payload.messages.forEach(function(item) {
				szSet.push((typeof payload.serializer !== 'undefined' ? payload.serializer.serialize(item) : item))
			})
			this.transport.encodeMessageSet(szSet, self.transport.compressions.none, function(mSet) {
				var topic = {
					TopicName: payload.topic,
					Partitions: [ {	Partition: payload.partition, MessageSet: mSet } ]
				}
				topics.push(topic);

				self.transport.produceRequest(broker, self.options.clientId, self.options.requiredAcks, self.options.produceTimeout, topics, function(response) {
					callback && callback(null, response);
				})
			})
		}
		else {
			var error = new Error('no broker found for ' + payload.topic)
			callback && callback(error, {});
		}
	}
}

module.exports = KafkaProducer;

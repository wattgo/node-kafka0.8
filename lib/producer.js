'use strict'

var KafkaTransport = require('./transport'),
	Events = require('events'),
	util = require('util')

var KafkaProducer = function (options, callback) {
	this.options = options || {};
	if(typeof this.options.requiredAcks === 'undefined') {
		this.requiredAcks = 1;
	}
	if(typeof this.options.timeout === 'undefined') {
		this.timeout = 1000 * 30;
	}
	if(typeof this.options.transport === 'undefined') {
		this.transport = new KafkaTransport(this.options);
	}
	if(typeof this.options.clientId === 'undefined') {
		this.clientId = this.transport.clientId;;
	}

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

				self.transport.produceRequest(broker, self.clientId, self.options.requiredAcks, self.options.timeout, topics, function(response) {
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

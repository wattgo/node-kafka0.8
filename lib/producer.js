'use strict'

var KafkaTransport = require('./transport'),
	Events = require('events'),
	util = require('util'),
	async = require('async');

var KafkaProducer = function (options, callback) {
	var self = this;
	self.options = options || {};
	self.options.requiredAcks = options.requiredAcks || 1
	self.options.produceTimeout = 1000 * 30;
    self.transport = options.transport || new KafkaTransport(self.options);
	self.options.clientId = options.clientId || self.transport.clientId;

	self.options.metadataTTL = self.options.metadataTTL || 5000;
	self.metadataExpired = true;
	self.metadata = {};

	self.transport.on('ready', function() {
		setInterval(function() {
			self.metadataExpired = true;
		}, self.options.metadataTTL)
		callback && callback();
	})
}

util.inherits(KafkaProducer, Events.EventEmitter);

KafkaProducer.prototype.refreshMetadata = function(callback) {
	var self = this;
	if(self.metadataExpired) {
		self.transport.refreshMetadata([], function(err, response) {
			if(!err) {
				self.metadata = response;
				self.metadataExpired = false;
				callback && callback(null, response);
			}
			else {
				callback && callback(err);
			}
		});
	}
	else {
		callback && callback(null, self.metadata);
	}
}

KafkaProducer.prototype.produce = function(payloads, callback) {
	var self = this;
	var request = [];
	var topics = [];

	self.refreshMetadata(function(err, response) {

		if(err) {
			return callback && callback(err);
		}

		async.eachSeries(payloads, function(payload, nextPayload) {

			async.eachSeries(self.metadata.Topics, function(topic, nextTopic) {

				if(topic.TopicName === payload.topic) {

					async.eachSeries(topic.Partitions, function(partition, nextPartition) {

						if(partition.Partition === payload.partition) {

							var brokerId = partition.Leader;

							self.transport.brokers.byId(brokerId, function(err, broker) {
								if(!err) {

									var messageSet = [];

									async.eachSeries(payload.messages, function(message, nextMessage) {
										messageSet.push((typeof payload.serializer !== 'undefined' ? payload.serializer.serialize(message) : message))
										nextMessage();
									}
									, function(done) {

										self.transport.encodeMessageSet(messageSet, self.transport.compressions.none, function(mSet) {

											var topic = {
												TopicName: payload.topic,
												Partitions: [ {	Partition: payload.partition, MessageSet: mSet } ]
											}
											request.push(topic);

											self.transport.produceRequest(broker, self.options.clientId, self.options.requiredAcks, self.options.produceTimeout, request, function(response) {

												nextPartition();
												//callback && callback(null, response);

											})
										})

									})
								}
								else {
									nextPartition();
								}

							})

						}
						else {
							nextPartition();
						}

					}
					, function(done) {
						nextTopic();
					})

				}
				else {
					nextTopic();
				}

			}
			, function(done) {
				nextPayload();
			})

		}
		, function(done) {
			callback && callback()
		})
	})
}

module.exports = KafkaProducer;

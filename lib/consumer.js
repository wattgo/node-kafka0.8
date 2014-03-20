'use strict'

var KafkaTransport = require('./transport'),
	async = require('async'),
	Binary = require('binary'),
	util = require('util'),
	Events = require('events'),
	memoryStore = require('./store/memory');

var KafkaConsumer = function (options, callback) {
	var self = this;

	self.options = options || {};

	self.state = self.options.transport ? 0 : -1;

	self.transport = self.options.transport || new KafkaTransport(options)

	self.options.clientId = self.options.clientId || self.transport.options.clientId;
	self.options.group = self.options.group || self.transport.options.group;
	self.options.maxBytes = self.options.maxBytes || (1024 * 1024);
	self.options.maxWaitTime = self.options.maxWaitTime || 10 * 3000;
	self.options.minBytes = self.options.minBytes || 0;
	self.options.metadataTTL = self.options.metadataTTL || 5000;

	self.metadataExpired = true;
	self.metadata = {}

	self.payloads = options.payloads || [];
	self.topicsToRequest = [];

	typeof options.store === 'undefined'
		? self.store = new memoryStore()
		: self.store = options.store;

	self.store.init();

	self.store.on('ready', function() {
		//console.log('[kafka consumer] store ready');
		++self.state > 0 && callback && callback();
	})

	self.transport.on('ready', function() {
		setInterval(function() {
			//console.log('[kafka consumer] metadata expired (ttl ' + self.options.metadataTTL + ')');
			self.metadataExpired = true;
		}, self.options.metadataTTL)
		++self.state > 0 && callback && callback();
	})

	self.transport.on('brokersChanged', function(brokers) {	
		self.metadataExpired = true;
	})

}

util.inherits(KafkaConsumer, Events.EventEmitter);

KafkaConsumer.prototype.updatePayloads = function(payloads) {
	this.payloads = payloads;
}

KafkaConsumer.prototype.addPayload = function(payload) {
	this.payloads.push(payload);
}

KafkaConsumer.prototype.refreshMetadata = function(callback) {
	var self = this;
	if(self.metadataExpired) {
		self.metadataTopicsToRequest(function(err, topics) {
			self.transport.refreshMetadata(topics, function(err, response) {
				if(!err) {
					self.metadata = response;
					self.metadataExpired = false;
					callback && callback(null, response);
				}
				else {
					callback && callback(err);
				}
			});
		})
	}
	else {
		callback && callback(null, self.metadata);
	}
}

/*
 *	if we have to consume by regex (or function)
 *	we need to fetch metadata for all topics
 */
KafkaConsumer.prototype.metadataTopicsToRequest = function(callback) {
	var toRequest = [];
	if(typeof this.payloads !== 'undefined' || !(this.payloads instanceof Array)) {
		async.each(this.payloads, function(payload, nextPayload) {
			var topics = (payload.topic instanceof Array ? payload.topic : [ payload.topic ]);
			async.each(topics, function(topic, nextTopic) {
				if(typeof topic !== 'string') {
					toRequest = [];
					nextTopic(true);
				}
				else {
					toRequest.push({ TopicName: topic });
					nextTopic();
				}
			}, nextPayload)
		}, function(done) {
			callback(null, (done ? [] : toRequest))
		})
	}
	else {
		callback(new Error("invalid array"), null)
	}
}


/*
 *	is this evil ?
 */
KafkaConsumer.prototype.prepare = function(callback) {
	var self = this;
	var requests = {}
	var brokers = [];

	if(typeof self.payloads !== 'undefined' || !(self.payloads instanceof Array)) {

		self.refreshMetadata(function(err, metadata) {

			if(err) {
				//console.log('[kafka consumer] prepare ', err);
				return callback && callback(err);
			}

			async.eachSeries(self.payloads, function(payload, nextPayload) {

				var topics = (payload.topic instanceof Array ? payload.topic : [ payload.topic ]);
				var partitions = (payload.partition instanceof Array ? payload.partition : [ payload.partition ])

				async.eachSeries(topics, function(payloadTopic, nextTopic) {

					async.eachSeries(metadata.Topics, function(metadataTopic, nextMetadataTopic) {

						var metadataTopicName = metadataTopic.TopicName;
						var isTopicToConsume = ((typeof payloadTopic === 'string' && payloadTopic === metadataTopicName) || (payloadTopic instanceof RegExp && payloadTopic.test(metadataTopicName) === true) || (typeof payloadTopic === 'function' && payloadTopic(metadataTopicName, metadataTopic) === true))
						if(isTopicToConsume && !metadataTopic.TopicErrorCode) {

							async.eachSeries(partitions, function(payloadPartition, nextPartition) {

								async.eachSeries(metadataTopic.Partitions, function(metadataPartition, nextMetadataPartition) {

									var payloadPartitionId;
									var payloadPartitionOffset;
									if(typeof payloadPartition === 'object' && typeof payloadPartition.id !== 'undefined') {
										payloadPartitionId = payloadPartition.id;
										if(typeof payloadPartition.offset !== 'undefined') {
											payloadPartitionOffset = payloadPartition.offset;
										}
									}
									else {
										payloadPartitionId = payloadPartition;
									}

									var isPartitionToConsume = ( (typeof payloadPartitionId === 'number' && payloadPartitionId === metadataPartition.Partition) || (typeof payloadPartitionId === 'function' && payloadPartitionId(metadataPartition.Partition, metadataTopicName, metadataTopic) === true) );
									if(isPartitionToConsume && !metadataPartition.PartitionErrorCode) {

										var brokerId = metadataPartition.Leader;

										if(brokers.indexOf(brokerId) < 0) {
											brokers.push(brokerId);
										}

										if(typeof requests[brokerId] === 'undefined') {
											requests[brokerId] = {
												broker: 0,
												requests: [],
												topics: {}
											};
										}

										self.transport.brokers.byId(brokerId, function(err, broker) {
											if(!err) {
												requests[brokerId].broker = broker;

												var req = requests[brokerId].requests;
												var tpc = requests[brokerId].topics;
												var prt = metadataPartition.Partition;

												if(typeof tpc[metadataTopicName] !== 'undefined' && typeof tpc[metadataTopicName][prt] !== 'undefined') {
													nextMetadataPartition();
												}
												else {
													if(typeof tpc[metadataTopicName] === 'undefined') {
														tpc[metadataTopicName] = {};
													}

													tpc[metadataTopicName][prt] = {
														each: (typeof payload.each !== 'undefined' ? payload.each : null),
														done: (typeof payload.done !== 'undefined' ? payload.done : null),
														group: (typeof payload.group !== 'undefined' ? payload.group : self.options.group),
														offset: payloadPartitionOffset
													}

													if(typeof payloadPartitionOffset !== 'undefined') {
														req.push({
															TopicName: metadataTopicName,
															Partitions: [{
																Partition: prt,
																FetchOffset: payloadPartitionOffset,
																MaxBytes: self.options.maxBytes
															}]
														})
														nextMetadataPartition();
													}
													else {
														self.store.fetchOffset(tpc[metadataTopicName][prt].group, metadataTopicName, prt, function(lastOffset) {
															var fetchOffset = lastOffset + 1;
															req.push({
																TopicName: metadataTopicName,
																Partitions: [{
																	Partition: prt,
																	FetchOffset: fetchOffset,
																	MaxBytes: self.options.maxBytes
																}]
															});
															tpc[metadataTopicName][prt].offset = fetchOffset;
															nextMetadataPartition();
														});
													}
												}
											}
											else {
												nextMetadataPartition();
											}
										})
									}
									else {
										nextMetadataPartition();
									}

								}, nextPartition)

							}, nextMetadataTopic)
						}
						else {
							nextMetadataTopic();
						}
					}, nextTopic)

				}, nextPayload)

			}, function(done) {
				callback(null, brokers, requests);
			})
		})
	}
	else {
		callback(new Error("invalid array"))
	}
}

KafkaConsumer.prototype.consume = function(eachCallback, doneCallback, endCallback) {
	var self = this;

	self.prepare(function(err, brokers, requests) {

		if(err) {
			//console.log('[kafka consumer] consume', err);
			return endCallback && endCallback(err);
		}

		async.each(brokers, function(brokerId, nextBroker) {
			var current = requests[brokerId];
			if(typeof current.broker !== 'undefined') {
				self.transport.fetchRequest(current.broker, self.options.clientId, -1, self.options.maxWaitTime, self.options.minBytes, current.requests, function(response) {

					if(response.Topics.length) {

						async.eachSeries(response.Topics, function(topic, nextTopic) {

							async.eachSeries(topic.Partitions, function(partition, nextPartition) {

								var currentPartition = current.topics[topic.TopicName][partition.Partition];
								var startOffset = currentPartition.offset;

								self.transport.decodeMessageSet(partition.MessageSet, function(buffer, message, nextCallback) {
									var msg;
									// TODO: wrap deserialize in callback before storing offset
									try {
										msg = serializer.deserialize(buffer);
									}
									catch(e) {
										msg = buffer.toString();
									}

									var each = (currentPartition.each ? currentPartition.each : eachCallback);
									each(msg, { topic: topic.TopicName, partition: partition.Partition, offset: startOffset }, function() {
										self.store.commitOffset(currentPartition.group, topic.TopicName, partition.Partition, startOffset, function() {
											if(startOffset < partition.HighwaterMarkOffset) {
												startOffset++;
											}
											currentPartition.offset = startOffset;
											async.eachSeries(current.requests, function(req, nextReq) {
												if(req.TopicName === topic.TopicName) {
													async.eachSeries(req.Partitions, function(part, nextPart) {
														if(part.Partition === partition.Partition) {
															part.FetchOffset = startOffset;
															nextPart(true);
														}
														else {
															nextPart();
														}
													}, function(err) {
														nextReq(true);
													})
												}
												else {
													nextReq();
												}
											}, function() {
												nextCallback();
											})
										});
									})
								},
								function() {
									var done = (currentPartition.done ? currentPartition.done : doneCallback)
									partition.MessageSetSize && done && done();
									nextPartition();
								})

							}, nextTopic)

						}, nextBroker)

					}
					else {
						nextBroker();
					}
				}, function() {
					endCallback && endCallback();
				});

			}
		}, function() {
			endCallback && endCallback();
		})

	})

};

module.exports = KafkaConsumer;

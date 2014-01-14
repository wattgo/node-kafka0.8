'use strict'

var util = require('util'),
	KafkaProtocol = require('./protocol'),
	KafkaBrokers = require('./brokers'),
	net = require('net'),
	async = require('async'),
	Binary = require('binary'),
	Events = require('events'),
	BufferMaker = require('buffermaker'),
	zlib = require('zlib'),
	snappy = require('snappy');

	var fs = require('fs');

var KafkaTransport = function (options, callback) {
	var self = this;

	this.startup = true;
	this.clientId = this.defaultClientId;
	this.compression = options.compression || 'none';
	this.correlation = {};
	this.setMaxListeners(0);

	this.on('ready', function() {
		callback && callback();
	})

	this.brokers = new KafkaBrokers();
	this.brokers.on('data', function(broker, data) {
		self.response(broker, data);
	})

	if(typeof options.zkClient !== 'undefined') {
		this.zkClient = options.zkClient;
		this.zkClient.on('ready', function(brokers) {
			self.updateBrokerList(brokers)
		});
		this.zkClient.on('zkBrokersChanged', function(brokers) {
			self.brokers.clean(brokers);
			self.updateBrokerList(brokers);
		});
		this.zkClient.zk.connect();
	}
	else if(typeof options.brokers !== 'undefined') {
		this.updateBrokerList(options.brokers);
	}
	else {
		throw new Error('please specify at least one broker')
	}
}

util.inherits(KafkaTransport, KafkaProtocol);

KafkaProtocol.prototype.updateBrokerList = function(brokers, callback) {
	var self = this;
	async.forEach(brokers, function(broker, cb) {
		self.brokers.add(broker);
		cb();
	}, function(done) {
		var randomBroker = self.brokers.random();
		if(typeof randomBroker !== 'undefined') {
			self.metadataRequest(randomBroker, self.clientId, [], function(response) {
				async.forEach(response.Brokers,	function(broker, cb) {
					self.brokers.add(broker);
					cb();
				}, function(done) {
					self.topics = response.Topics;
					if(self.startup) {
						self.startup = false;
						self.emit('ready');
					}
					callback && callback();
				})
			})
		}
	})
}

KafkaTransport.prototype.getCorrelationId = function() {
	var cid = 0;
	while(!cid) {
		cid = Math.floor(Math.random() * 2147483647);
	}
	return cid;
}

KafkaTransport.prototype.response = function(broker, data) {
	if(typeof broker.lastHeader === 'undefined') { // buffering ?
		var bin = Binary.parse(data);
		broker.lastHeader = this.decode(this.schema.ResponseHeader, bin);
		if(data.length > broker.lastHeader.ResponseSize) { // full response ?
			var lastRequest = broker.requested.shift();
			var response = this.decode(this.schema[lastRequest.api].Response, bin);
			lastRequest.callback(response);
			delete broker.lastHeader;
		}
		else { // partial response
			broker.lastBuffer = new Buffer(data);
		}
	}
	else {

		broker.lastBuffer = Buffer.concat([ broker.lastBuffer, data ]);
		if(broker.lastBuffer.length > broker.lastHeader.ResponseSize) { // end of response
			var bin = Binary.parse(broker.lastBuffer);
			var header = this.decode(this.schema.ResponseHeader, bin);
			var lastRequest = broker.requested.shift();
			var response = this.decode(this.schema[lastRequest.api].Response, bin);
			lastRequest.callback(response);
			delete broker.lastBuffer;
			delete broker.lastHeader;
		}
	}
}

KafkaTransport.prototype.request = function (broker, api, clientId) {
   	var args = Array.prototype.slice.call(arguments, 3, -1);
	var correlationId = this.getCorrelationId()
	if(!(api === 'Produce' && args[0] === this.ackTypes.NO_RESPONSE)) { // requiredAcks not set to 0, server will answer
		broker.requested.push({
			cid: correlationId,
			api: api,
			callback: arguments[arguments.length - 1]
		});
	}
    args.unshift(this.schema[api].Request);
    var requestHeader = this.encode(this.schema.RequestHeader, this.apiKeys[api], this.apiVersion, correlationId, clientId);
    var request = this.encode.apply(this, args);
    var header = this.encode(this.schema.Header, Buffer.concat([requestHeader.make(), request.make()]))
    broker.socket.write(header.make());
}

KafkaTransport.prototype.metadataRequest = function (broker, clientId, topics, callback) {
	this.request(broker, 'Metadata', clientId, topics, callback);
}

KafkaTransport.prototype.produceRequest = function(broker, clientId, requiredAcks, timeout, topics, callback) {
	this.request(broker, 'Produce', clientId, requiredAcks, timeout, topics, callback);
}

KafkaTransport.prototype.fetchRequest = function(broker, clientId, replicaId, maxWaitTime, minBytes, topics, callback) {
	this.request(broker, 'Fetch', clientId, replicaId, maxWaitTime, minBytes, topics, callback);
}

KafkaTransport.prototype.offsetRequest = function(broker, clientId, replicaId, topics) {
	this.request(broker, 'Offset', clientId, replicaId, topics)
}

KafkaTransport.prototype.encodeMessageSet = function(messages, compression, callback) {
	var self = this;
	var set = new Buffer(0);

	messages.forEach(function(item) {
		var m = self.encode(self.schema.Message, 0/*crc*/, self.magicByte, compression, 0/*key*/, item);
		var mheader = self.encode(self.schema.MessageHeader, 0, m.make());
		set = Buffer.concat([ set, mheader.make() ])
	})

	if(this.compression !== 'none' && compression === this.compressions.none) {
		switch(this.compression) {
			case 'gzip': 
				zlib.gzip(set, function(err, gzipped) {
					self.encodeMessageSet([ gzipped ], self.compressions.gzip, function(compressed) {
						callback(compressed);
					})
				})
			break;
			case 'snappy':
				var snapped = snappy.compressSync(set); // cant use async compress due to node-snappy bug
				self.encodeMessageSet([ snapped ], self.compressions.snappy, function(compressed) {
					callback(compressed);
				})
			break;
		}
	}
	else {
		callback(set);
	}
}

KafkaTransport.prototype.decodeMessageSet = function(bytesOrBin, callback, end) {
	var self = this;
	var bin;
	bytesOrBin instanceof Buffer
		? bin = Binary.parse(bytesOrBin)
		: bin = bytesOrBin

	if(!bin.eof()) {
		var header = this.decode(this.schema.MessageHeader, bin);
		var msg = this.decode(this.schema.Message, Binary.parse(header.Message));
		switch(msg.Attributes) {
			case this.compressions.gzip:
				try {
					zlib.gunzip(msg.Value, function(err, gunzipped) {				
						self.decodeMessageSet(gunzipped, callback, end);
					})
				}
				catch(e) {
					console.log('zlib decompression error:', e);
				}
			break;
			case this.compressions.snappy:
				try {
					snappy.decompress(msg.Value, snappy.parsers.raw, function(err, unsnapped){
						self.decodeMessageSet(unsnapped, callback, end);
					});
				}
				catch(e) {
					console.log('snappy decompression error:', e)
				}
			break;
			default:
				callback(msg.Value, msg, function() {
					self.decodeMessageSet(bin, callback, end);
				});
		}
	}
	else {
		end();
	}
}

module.exports = KafkaTransport;

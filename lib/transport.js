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

var KafkaTransport = function (options, callback) {
	var self = this;

	this.startup = true;
	this.correlation = {};
	this.setMaxListeners(0);

	this.options = options || {}
	this.options.clientId = this.options.clientId || this.defaultClientId;
	this.options.group = this.options.group || this.defaultGroup;
	this.options.compression = this.options.compression || 'none';
	this.options.timeout = this.options.timeout || 5000;

	this.brokers = new KafkaBrokers(options);
	this.brokers.on('data', function(broker, data) {
		self.response(broker, data);
	})

	if(typeof options.zkClient !== 'undefined') {
	
		this.zkClient = options.zkClient;

		this.zkClient.on('ready', function(brokers) {
			self.refreshBrokers(brokers);
		})

		this.zkClient.on('zkBrokersChanged', function(brokers) {
			self.refreshBrokers(brokers);
		})

		this.zkClient.zk.connect();
	}
	else if(typeof options.brokers !== 'undefined') {
		var brokers = [];
		async.eachSeries(options.brokers, function(item, next) {
			var meta = {
				host: 'localhost',
				port: 9092
			}
			var tmp = item.split(':');
			meta.host = tmp[0];
			if(typeof tmp[1] !== 'undefined') {
				meta.port = tmp[1];
			}
			brokers.push(meta);
		}
		, function(done) {
			self.refreshBrokers(brokers);
		})
	}

	else {
		throw new Error('please specify at least one broker')
	}
}

util.inherits(KafkaTransport, KafkaProtocol);

KafkaTransport.prototype.refreshBrokers = function(brokers) {
	var self = this;
	async.eachSeries(brokers, function(item, next) {
		self.brokers.create(item, function(broker) {
			next();
		})
	}
	, function(done) {
		if(self.startup) {
			console.log('[kafka transport] ready')
			self.startup = false;
			self.emit('ready', brokers);
		}
		else {
			console.log('[kafka transport] brokers changed');
			self.emit('brokersChanged', brokers);
		}
	})
}

KafkaTransport.prototype.refreshMetadata = function(topics, callback) {
	var self = this;
	self.brokers.random(function(err, broker) {
		if(!err) {
			self.metadataRequest(broker, self.options.clientId, topics, function(response) {
				if(typeof response.Brokers !== 'undefined' && response.Brokers.length) {
					async.eachSeries(response.Brokers, function(item, next) {
						self.brokers.create(item, function(newbroker) {
							self.brokers.addTopics(newbroker, response.Topics, function(err) {
								next();
							});
						})
					}
					, function(done) {
						callback && callback(null, response);
					})

				}
				else {
					callback && callback(new Error('no brokers found'))
				}

			}
			, function(err) {
				
			})
		}
		else {
			callback && callback(new Error('no broker found'))
		}

	})
}

KafkaTransport.prototype.response = function(broker, data) {
	var self = this;
	if(typeof broker !== 'undefined') {
		if(!broker.response.buffering) {
			var bin = Binary.parse(data);
			var header = this.decode(this.schema.ResponseHeader, bin);
			if(data.length > header.ResponseSize) {
				//var req = broker.requests.shift();
				self.getRequest(broker, header.CorrelationId, function(err, req, index) {
					if(!err) {
						var response = self.decode(self.schema[req.api].Response, bin);
						req.callback(response);
						broker.requests.splice(index, 1);
					}
				})
			}
			else {
				broker.response = {
					buffering: true,
					buffer: new Buffer(data),
					bytesToRead: header.ResponseSize
				}
			}
		}
		else {
			broker.response.buffer = Buffer.concat([ broker.response.buffer, data ]);
			if(broker.response.buffer.length > broker.response.bytesToRead) {
				var bin = Binary.parse(broker.response.buffer);
				var header = this.decode(this.schema.ResponseHeader, bin);
				//var req = broker.requests.shift();
				self.getRequest(broker, header.CorrelationId, function(err, req, index) {
					var response = self.decode(self.schema[req.api].Response, bin);
					req.callback(response);
					broker.response.buffering = false; 
					broker.requests.splice(index, 1);
				})
			}
		}
	}
}

KafkaTransport.prototype.getRequest = function(broker, cid, callback) {
	var index = -1;
	async.eachSeries(broker.requests, function(item, next) {
		index++;
		item.cid === cid
			? next(item)
			: next()
	}
	, function(request) {
		callback(request ? null : new Error('request not found'), request, index);
	})
}

KafkaTransport.prototype.request = function (broker, api, clientId) {
	var self = this;
   	var args = Array.prototype.slice.call(arguments, 3, -1);
	var cid = Math.floor(Math.random() * 2147483647);
	if(!(api === 'Produce' && args[0] === this.ackTypes.NO_RESPONSE)) { // requiredAcks not set to 0, server will answer
		broker.requests.push({
			cid: cid,
			api: api,
			callback: arguments[arguments.length - 2],
			endCallback: arguments[arguments.length - 1]
		});

		setTimeout(function() {
			self.getRequest(broker, cid, function(err, request, index) {
				if(!err) {
					request.endCallback(new Error('request timeout'));
					broker.requests.splice(index, 1);
				}
			})
		}, this.options.timeout)
	}
    args.unshift(this.schema[api].Request);
    var requestHeader = this.encode(this.schema.RequestHeader, this.apiKeys[api], this.apiVersion, cid, clientId);
    var request = this.encode.apply(this, args);
    var header = this.encode(this.schema.Header, Buffer.concat([requestHeader.make(), request.make()]))
    broker.socket.write(header.make());
}

KafkaTransport.prototype.metadataRequest = function (broker, clientId, topics, callback, endCallback) {
	this.request(broker, 'Metadata', clientId, topics, callback, endCallback);
}

KafkaTransport.prototype.produceRequest = function(broker, clientId, requiredAcks, timeout, topics, callback, endCallback) {
	this.request(broker, 'Produce', clientId, requiredAcks, timeout, topics, callback, endCallback);
}

KafkaTransport.prototype.fetchRequest = function(broker, clientId, replicaId, maxWaitTime, minBytes, topics, callback, endCallback) {
	this.request(broker, 'Fetch', clientId, replicaId, maxWaitTime, minBytes, topics, callback, endCallback);
}

/*
KafkaTransport.prototype.offsetRequest = function(broker, clientId, replicaId, topics, callback, endCallback) {
	this.request(broker, 'Offset', clientId, replicaId, topics, callback, endCallback)
}
*/

KafkaTransport.prototype.encodeMessageSet = function(messages, compression, callback) {
	var self = this;
	var set = new Buffer(0);

	messages.forEach(function(item) {
		var m = self.encode(self.schema.Message, 0/*crc*/, self.magicByte, compression, 0/*key*/, item);
		var mheader = self.encode(self.schema.MessageHeader, 0, m.make());
		set = Buffer.concat([ set, mheader.make() ])
	})

	if(this.options.compression !== 'none' && compression === this.compressions.none) {
		switch(this.options.compression) {
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
					console.log('[kafka transport] decodeMessageSet zlib decompression error', e);
				}
			break;
			case this.compressions.snappy:
				try {
					snappy.decompress(msg.Value, snappy.parsers.raw, function(err, unsnapped) {
              			self.decodeMessageSet(unsnapped, callback, end);
					});
				}
				catch(e) {
					console.log('[kafka transport] decodeMessageSet snappy decompression error', e)
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

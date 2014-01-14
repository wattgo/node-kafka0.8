'use strict'

var BufferMaker = require('buffermaker'),
	Binary = require('binary'),
	CRC32 = require('buffer-crc32'),
	util = require('util'),
	events = require('events');

var KafkaBuffer = function() {

}

util.inherits(KafkaBuffer, events.EventEmitter)

KafkaBuffer.prototype.encode = function (schema) {
	var buffer = new BufferMaker();
	var i = 1;
	var crc = false;
	var self = this;

	for(var k in schema) {
		if(schema[k] !== 'Crc32') {
			if(typeof schema[k] === 'string') {
				this['enc' + schema[k]](buffer, arguments[i])
			}
			else {
				buffer.UInt32BE(arguments[i].length);
				arguments[i].forEach(function(item) {
					var args = [ schema[k] ];
					for(var s in schema[k]) {
						args.push(item[s]);
					}
					var b = self.encode.apply(self, args);
					buffer.string(b.make());
				});
			}
		}
		else {
			crc = true
		}
		i++;
	}

	if(crc) {
		return this.encCrc32(buffer.make())
	}

	return buffer;
}

KafkaBuffer.prototype.decode = function (schema, binary) {
	var self = this;
	var decoded = {};
	for(var k in schema) {
		if(typeof schema[k] === 'string') {
			self['dec' + schema[k]](binary, k);
			if(schema[k] === 'String') {
				decoded[k] = binary.vars[k].toString()
			}
			else {
				decoded[k] = binary.vars[k];
				if(schema[k] === 'Bytes') {
					decoded[k + 'Size'] = binary.vars[k + 'Size'];
				}
			}
		}
		else {
			if(typeof decoded[k] === 'undefined') {
				decoded[k] = [];
			}
			binary
				.word32bs(k + 'Count')
				.tap(function (v) {
					for(var i = 0; i < v[k + 'Count']; i++) {
						var ret = self.decode(schema[k], this)
						decoded[k].push(ret)
					}
				})
		}
	}
	return decoded;
}

KafkaBuffer.prototype.encCrc32 = function (string) {
	return new BufferMaker().UInt32BE(CRC32.unsigned(string)).string(string);
}

KafkaBuffer.prototype.decCrc32 = function (binary, key) {
	binary.word32bu(key);
}

KafkaBuffer.prototype.encInt64 = function (buffer, value) {
	buffer.Int64BE(value)
}
KafkaBuffer.prototype.decInt64 = function (binary, key) {
	binary.word64bu(key);
}

KafkaBuffer.prototype.encUInt32 = function (buffer, value) {
	buffer.UInt32BE(value)
}
KafkaBuffer.prototype.decUInt32 = function (binary, key) {
	binary.word32bu(key);
}

KafkaBuffer.prototype.encInt32 = function (buffer, value) {
	buffer.Int32BE(value)
}
KafkaBuffer.prototype.decInt32 = function (binary, key) {
	binary.word32bs(key);
}

KafkaBuffer.prototype.encUInt16 = function (buffer, value) {
	buffer.UInt16BE(value);
}
KafkaBuffer.prototype.decUInt16 = function (binary, key) {
	binary.word16bu(key);
}

KafkaBuffer.prototype.encInt16 = function (buffer, value) {
	buffer.Int16BE(value);
}
KafkaBuffer.prototype.decInt16 = function (binary, key) {
	binary.word16bs(key);
}

KafkaBuffer.prototype.encUInt8 = function (buffer, value) {
	buffer.UInt8(value);
}
KafkaBuffer.prototype.decUInt8 = function (binary, key) {
	binary.word8bu(key);
}

KafkaBuffer.prototype.encString = function (buffer, value) {
	buffer.UInt16BE(value.length).string(value);
}
KafkaBuffer.prototype.decString = function (binary, key) {
	binary.word16bu(key + 'Length').buffer(key, key + 'Length')
}

KafkaBuffer.prototype.encBytes = function (buffer, value) {
	buffer.UInt32BE(value.length).string(value);
}
KafkaBuffer.prototype.decBytes = function (binary, key) {
	binary.word32bu(key + 'Size').buffer(key, key + 'Size');
}

module.exports = KafkaBuffer;

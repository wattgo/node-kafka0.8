'use strict'

var util = require('util'),
	KafkaBuffer = require('./buffer');

var KafkaProtocol = function () {

}

util.inherits(KafkaProtocol, KafkaBuffer);

KafkaProtocol.prototype.defaultClientId = 'default-node-client';

KafkaProtocol.prototype.defaultGroup = 'wattgo-node'

KafkaProtocol.prototype.apiVersion = 0;

KafkaProtocol.prototype.magicByte = 0;

KafkaProtocol.prototype.compressions = {
	none: 0,
	gzip: 1,
	snappy: 16
}

KafkaProtocol.prototype.apiKeys = {
	Produce: 0,
	Fetch: 1,
	Offset: 2,
	Metadata: 3,
	LeaderAndlsr: 4,
	StopReplica: 5
}

KafkaProtocol.prototype.errorCodes = {
	'-1': 'Unknown',
	'0'	: 'NoError',
	'1'	: 'OffsetOutOfRange',
	'2'	: 'InvalidMessage',
	'3'	: 'UnknownTopicOrPartition',
	'4'	: 'InvalidMessageSize',
	'5'	: 'LeaderNotAvailable',
	'6'	: 'NotLeaderForPartition',
	'7'	: 'RequestTimedOut',
	'8'	: 'BrokerNotAvailable',
	'9'	: 'ReplicaNotAvailable',
	'10': 'MessageSizeTooLarge',
	'11': 'StaleControllerEpochCode',
	'12': 'OffsetMetadataTooLargeCode'
}

KafkaProtocol.prototype.ackTypes = {
	NO_RESPONSE: 0,
	WAIT_LOCAL_WRITE: 1,
	BLOCK_UNTIL_COMMIT: -1
}

KafkaProtocol.prototype.schema = {

	Header: {
		Message: 'Bytes'
	},

	RequestHeader: {
		ApiKey: 'UInt16',
		ApiVersion: 'UInt16',
		CorrelationId: 'UInt32',
		ClientId: 'String'
	},

	ResponseHeader: {
		ResponseSize: 'UInt32',
		CorrelationId: 'UInt32'
	},

	MessageHeader: {
  		Offset: 'Int64',
  		Message: 'Bytes'		
	},

	Message: {
		Crc: 'Crc32',
		MagicByte: 'UInt8',
		Attributes: 'UInt8',
		Key: 'UInt32',
		Value: 'Bytes'
	},

	/*
	 *	request / response
	 */

	Metadata: {
		Request: {
			Topics: {
				TopicName: 'String'
			}
		},
		Response: {
			Brokers: {
				nodeId: 'UInt32',
				host: 'String',
				port: 'UInt32'
			},
			Topics: {
				TopicErrorCode: 'UInt16',
				TopicName: 'String',
				Partitions: {
					PartitionErrorCode: 'UInt16',
					Partition: 'UInt32',
					Leader: 'UInt32',
					Replicas: {
						NodeId: 'UInt32'
					},
					Lsrs: {
						Lsr: 'UInt32'
					}
				}
			}
		}
	},

	Produce: {
		Request: {
			RequiredAcks: 'Int16',
			Timeout: 'UInt32',
			Topics: {
				TopicName: 'String',
				Partitions: {
					Partition: 'UInt32',
					MessageSet: 'Bytes'
				}
			}
		},
		Response: {
			Topics: {
				TopicName: 'String',
				Partitions: {
					Partition: 'UInt32',
					ErrorCode: 'UInt16',
					Offset: 'Int64'
				}
			}
		}
	},

	Fetch: {
		Request: {
			ReplicaId: 'Int32',
			MaxWaitTime: 'UInt32',
			MinBytes: 'UInt32',
			Topics: {
				TopicName: 'String',
				Partitions: {
					Partition: 'UInt32',
					FetchOffset: 'Int64',
					MaxBytes: 'UInt32'
				}
			}
		},
		Response: {
			Topics: {
				TopicName: 'String',
				Partitions: {
					Partition: 'UInt32',
					ErrorCode: 'UInt16',
					HighwaterMarkOffset: 'Int64',
					MessageSet: 'Bytes'
				}
			}
		}
	},

	Offset: {
		Request: {
			ReplicaId: 'UInt32',
			Topics: {
				TopicName: 'String',
				Partitions: {
					Partition: 'UInt32',
					Time: 'Int64',
					MaxNumberOfOffsets: 'UInt32'
				}
			}

		},
		Response: {
			Topics: {
				TopicName: 'String',
				PartitionOffsets: {
					Partition: 'UInt32',
					ErrorCode: 'UInt16',
					Offsets: {
						Offset: 'Int64'
					}
				}
			}
		}
	}

}

module.exports = KafkaProtocol;
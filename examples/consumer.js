#!/usr/local/bin/node

var Kafka = require('../lib/kafka');

var zkClient = new Kafka.Zookeeper();

var transport = new Kafka.Transport({
    zkClient: zkClient
})

var stringSerializer = new Kafka.Serializer.String();
var jsonSerializer = new Kafka.Serializer.Json();

var topic = process.argv[2];

var consumer = new Kafka.Consumer({
	transport: transport,
	store: new Kafka.Store.Zookeeper({ zkClient: zkClient }),
	payloads: [
		{
			topic: [ 'test', 'hello' ],
			group: 'test-group',
			serializer: stringSerializer,
			partition: [ function(partitionId, topic, meta) { return partitionId < 8 } ]
		}
	]
}
, function() {

	function do_consume() {

		consumer.consume(function(msg, meta, next) {
			console.log('Topic:', meta.topic, '- Partition:', meta.partition, '- Offset:', meta.offset, '- Message:', msg);
			next();
		}
		, function() {
			console.log('end of message set');
		}
		, function(err) {
			if(err) {
				console.log(err);
			}
			setTimeout(function() {
				do_consume();
			}, 1000)
		});
	}

	do_consume();

})

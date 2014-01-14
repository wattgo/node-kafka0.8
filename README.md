Kafka 0.8 node client
=====================

Features
--------
* Optional Zookeeper
* Gzip and Snappy compression
* Auto-commit offset
* Custom offset store (memory, redis, zookeeper soon, ... buildYourOwn)
* Custom serializer (string, json, avro soon, ... buildYourOwn)

Transport (for both consumer and producer)
------------------------------------------

1. Zookeeper support :

	var transport = new Kafka.Transport({
		zkClient: new Kafka.Zookeeper()
		compression: 'snappy'
	})

2. Brokers only support :

	var transport = new Kafka.Transport({
		brokers: [ 'broker01', 'broker02:9898' ]
	})

A random broker will be requested for metadata

Consumer
--------

To consume a topic, you need : 
* a transport layer
* an offsetStore
* a topic / partition
* a serializer
* a callback which get executed for each message
* an optional end callback

Example :

	var consumer = new Kafka.Consumer({
		transport: transport,
		offsetStore: new Kafka.OffsetStore.Redis(/* default node redis options*/)
	}
	, onReady)

	var topic = 'mytopic';
	var partition = 0;
	var serializer = new Kafka.Serializer.String();

	function onReady() {
		consumer.consume(topic, partition, serializer, function(message, offset, next) {
			console.log('consume:', message, offset);
			/*
			 *	next() will commit offset to the offsetStore
			 *	and fetch next message
			 */
			next();
		}
		, function() {
			// done with this message set, consume again in 5 sec !
			setTimeout(function() {
				onReady();
			}
			, 5000);
		})
	}

Producer
--------

	var producer = new Kafka.Producer({
	  transport: transport
	}, onReady);

	var topic = 'jsontopic';
	var partition = 0;
	var serializer = new Kafka.Serializer.Json();

	function onReady() {
		producer.produce({
		 	topic: topic,
	 		partition: partition,
	 		serializer: serializer,
	  		messages: [ { mykey: "Hello World!" } ] // 'messages' should only contains json objects !
		});
	}

OffsetStore
-----------

An offset store is a class implementing at least these 4 functions :

1. 'init' (emiting a 'ready' event when ready)

	customStore.prototype.init = function() {
		// ...
		this.emit('ready');
	}

2. 'fetch' :

	customStore.prototype.fetch = function(topic, group, partition, callback) {
		// ...
		callback(offset)
	}

3. 'commit' :

	customStore.prototype.commit = function(offset, topic, group, partition, callback) {
		// ...
	}

4. 'cleanup':

	customStore.prototype.celanup = function() {
		// not yet used
	}

Serializer
----------

A serializer is a class implementing at least the 'serialize' and 'deserialize' functions.
See JsonSerializer for example :

	kafkaJsonSerializer.prototype.deserialize = function(data) {
	  return JSON.parse(data);
	}

	kafkaJsonSerializer.prototype.serialize = function(json) {
	  return JSON.stringify(json);
	}

Compression
-----------
	
Snappy & Gzip compression support !
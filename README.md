Kafka 0.8 node client
=====================

Changes
-------
* New consumer api
* Consume multiple topics/partitions
* Consume topic by regex or function
* Zookeeper offset store now impemented
* Store re-design (should allow partitions subscription) /!\ format of offset key in redis changed /!\


TODO
----
* better producer API
* encode/decode message should be async
* auto partitions subscription for consumers group
* white_list / black_list of partitions
* add some partitioners (random, fnv, ...) ?


Features
--------
* Optional Zookeeper
* Gzip and Snappy compression
* Auto-commit offset
* Custom offset store (memory, redis, zookeeper, ... buildYourOwn)
* Custom serializer (string, json, avro soon, ... buildYourOwn)


Install
-------

**First**
```
npm install kafka0.8
```
**Then**
```
var Kafka = require('kafka0.8');
```


Transport (for both consumer and producer)
------------------------------------------

**Zookeeper support**
```
    var kTransport = new Kafka.Transport({
    	zkClient: new Kafka.Zookeeper()
		/* ... */
	})
```
**Brokers only support**
```
	var kTransport = new Kafka.Transport({
		brokers: [ 'broker01', 'broker02:9898' ]
		/* ... */
	})
```
A random broker will be requested for metadata

**Compression 'snappy' or 'gzip'**
```
	var kTransport = new Kafka.Transport({
		zkClient: new Kafka.Zookeeper(),
		compression: 'snappy'								/* or 'gzip' */
		/* ... */
	})
```

**Default options**
```
	{
		clientId: 'default-node-client',					/* default client id */
		group: 'wattgo-node',								/* default group */
		timeout: 5000,										/* request timeout (this is not the kafka one) */
		reconnectMs: 1000,									/* if we lost connection to a broker, try to reconnect every 1000ms */
		dnsLookup: true										/* perform a dns lookup on brokers hostname */
	}
```


Consumer
--------

**Default options**
```
	{
		clientId: 'my-consumer',							/* consumer id */
		group: 'my-group',									/* consumer group */

		maxBytes: 1024 * 1024,								/* max bytes to fetch per request */
		minBytes: 0,										/* min bytes to fetch per request */

		metadataTTL: 5000,									/* refresh metadata every 5000ms  */

		store: new Kafka.Store.Memory(),					/* offset and partition subscription store. see 'Store' section */

		payloads: [{										/* see 'Payloads' section for more advanced usages */
			topic: 'test',
			partition: 1,
			serializer: new Kafka.Serializer.Json()			/* we will parse json, see 'Serializer' section */
		}],

		/* optionally you can pass a transport layer */
		transport: kTransport,

		/* if you dont it will be created for you, pass your options here */
		timeout: 10000
		/* ... */
	}
```

**Usage**

```
var consumer = new Kafka.Consumer(options, function() {
	console.log('ready');
	consumer.consume(eachCallback, doneCallback, endCallback);
})
```

**eachCallback(msg, meta, next)** :<br/>
Executed for each message consumed<br/>
- msg: deserialized message<br/>
- meta: { topic, offset, partition }<br/>
- next: commit offset and get next message in message set, YOU HAVE TO CALL IT<br/>

**doneCallback()** :<br/> 
- executed at the end of message set<br/>

**endCallback(err)** :<br/>
- executed when everything has been consumed or if fetch request timed out<br/>

**Example**
```
var consumer = new Kafka.Consumer(options, do_consume);
function do_consume() {
	consumer.consume(
		function(msg, meta, next) {
			console.log('Message :', msg);
			console.log('Topic:', meta.topic, '- Partition:', meta.partition, '- Offset:', meta.offset);
			/* commit offset to offset store and get next message */
			next();
		},
		function() {
			console.log('end of this message set');
		},
		function(err) {
			console.log('done');
			// i can safely loop
			setTimeout(do_consume, 1000);
		}
	)
}
```

**Advanced payloads**

Each payload can define a topic, an array of topics, a regex to match topics name or a function to evaluate topic name at runtime.
You can do the same with 'partition' (except regex)
Optionally, if you have multiple payloads, you can provide some specific callbacks for some of them using 'each' and 'done' property.

Example :
```
payloads: [
	{
		topic: [
			'test',													// string
			/[0-9]+/,												// regex
			function(topicName, topicMetadata) {					// function returning true or false
				return topicName.length > 10
			}
		],
		partition: [
			3,														// partition id
			function(partitionId, topicName, topicMetadata) {		// function returning true or false
				return !(partitionId % 2)
			},
			{ id: 11, offset: 100 }									// partition id and start offset
		]
		each: function(msg, meta, next) {
			// executed for each message concerning this payload
		},
		done: function() {
			// end of message set
		}
	}
]
```


Producer
--------
```
	var producer = new Kafka.Producer({
	  transport: kTransport,
	  requiredAcks: 1,
	  produceTimeout: 1000 * 30
	}, onReady);

	var jsonSerializer = new Kafka.Serializer.Json();

	function onReady() {

    var payloads = [
      {
        topic: 'jsontopic',
        partition: 0,
        serializer: jsonSerializer,
        messages: [ { mykey: "Hello World" } ]
      }
    ]

		producer.produce(payloads, function() {
        // done
    })
	}
```

OffsetStore
-----------

An offset store is a class implementing at least these 4 functions :

**init** (emiting a 'ready' event when ready)
```
	customStore.prototype.init = function() {
		/* ... */
		this.emit('ready');
	}
```
**fetchOffset**
```
	customStore.prototype.fetchOffset = function(groupId, topic, partitionId, callback) {
		/* ... */
		callback(offset)
	}
```
**commitOffset**
```
	customStore.prototype.commitOffset = function(groupId, topic, partitionId, offset, callback) {
		/* ... */
    	callback(err);
	}
```
**cleanup**
```
	customStore.prototype.celanup = function() {
		/* not yet used */
	}
```
Serializer
----------

A serializer is a class implementing at least the 'serialize' and 'deserialize' functions.
See JsonSerializer for example :
```
	kafkaJsonSerializer.prototype.deserialize = function(data) {
	  return JSON.parse(data);
	}

	kafkaJsonSerializer.prototype.serialize = function(json) {
	  return JSON.stringify(json);
	}
```

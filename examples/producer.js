#!/usr/local/bin/node

var Fnv = require('fnv-plus');
var Kafka = require('../lib/kafka');

var fnv = new Fnv();

var num_partitions = 8;

// consistent hashing
function kPartitioner(key) {
  return fnv.hash(key).value % num_partitions;
}

var jsonSerializer = new Kafka.Serializer.Json();

var producer = new Kafka.Producer({
  zkClient: new Kafka.Zookeeper(),
  compression: 'gzip',
  clientId: 'client-node'
}, function() { // ready

  console.log('ready to produce...');

  var myId = 'abc1234';

  var i = parseInt(process.argv[2])

  var payloads = [{
    topic: 'test',
    partition: 0,
    serializer: jsonSerializer,
    messages: [ {i: i}, {a: 'b'} ]
  }]

  setInterval(function() {
    producer.produce(payloads, function() {
      payloads[0].messages[0].i = i;
      i++;
    });
  }, 1000);

});

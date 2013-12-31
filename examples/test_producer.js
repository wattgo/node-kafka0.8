var kafka = require('../kafka.js');

var producer = new kafka.Producer({
	zkClient: new kafka.Zookeeper()
})

var topic = 'mytopic';
var n_partitions = 4;
var keys = [ 'Wisdom', 'makes', 'light', 'the', 'darkness', 'of', 'ignorance' ]

function lengthPartitioner(k, n) {
	return k.length % n
}

keys.forEach(function(item) {
	console.log(item, 'will be produce on partition', lengthPartitioner(item, n_partitions))
})

setInterval(function() {

	var randomKey = keys[Math.floor(Math.random() * keys.length)]
	var partition = lengthPartitioner(randomKey, n_partitions)

	console.log('on', topic, 'key:', randomKey, '(partition ' + partition + ')');

	producer.produce({
		topic: topic,
		partition: partition,
		messages: [ randomKey, new Date().toString() ]
	}, function(error, response) {
		if(error) {
			console.log(error, response);
		}
	})

}, 500)

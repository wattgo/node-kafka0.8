var kafka = require('../kafka')

var tp = new kafka.Transport({
	zkClient: new kafka.Zookeeper()
})

var producer = new kafka.Producer({
	transport: tp,
	compression: 'gzip'
})
var consumer = new kafka.Consumer({
	transport: tp,
	offsetStore: new kafka.OffsetStore.redis()
});

var topic = 'mytopic';
var n_partitions = 4;
var keys = [ 'Wisdom', 'makes', 'light', 'the', 'darkness', 'of', 'ignorance' ]

function lengthPartitioner(k, n) {
	return k.length % n
}

setInterval(function() {
	var partition = 2;
	consumer.consume(topic, partition, function(msg, offset) {
		console.log('consumer:', msg, offset);
	})
}, 100);

setInterval(function() {

	var randomKey = keys[Math.floor(Math.random() * keys.length)]
	var partition = lengthPartitioner(randomKey, n_partitions)

	//console.log('producer:', randomKey, '(partition ' + partition + ')');

	producer.produce({
		topic: topic,
		partition: 2,
		messages: [ 'AAAAAAAAAAAA', 'AAAAAAAAAAAA' ]
	}, function(error, response) {
		if(error) {
			console.log(error, response);
		}
	})

}, 100)

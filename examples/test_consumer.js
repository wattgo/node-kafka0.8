var kafka = require('../lib/kafka.js')

var consumer = new kafka.Consumer({
	brokers: [ 'localhost' ],
	offsetStore: new kafka.OffsetStore.redis()
});

setInterval(function() {
	var partition = 0;
	consumer.consume('errors', partition, function(msg) {
		console.log(msg);
	})
}, 1000);

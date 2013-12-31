var kafka = require('../yakam')

var tp = new kafka.Transport({
	zkClient: new kafka.Zookeeper(),
	compression: 'gzip'
})

var consumer = new kafka.Consumer({
	transport: tp,
	offsetStore: new kafka.OffsetStore.memory()
});

var topic = 'gztopic';

setInterval(function() {
	var partition = 0;
	consumer.consume(topic, partition, function(msg, offset) {
		console.log('consumer:', msg, offset);
	})
}, 5000);


var producer = new kafka.Producer({
	transport: tp
},function() {
	producer.produce({
		topic: topic,
		partition: 0,
		messages: [ 'abcdef', '1234556', 'ARTYUIOKJHVBKKJH', 'foo.bar', 'HELLO', 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse in lorem blandit, convallis tellus blandit, feugiat justo. Proin at vestibulum sapien. Fusce enim sem, pulvinar at sollicitudin vel, feugiat vel nibh. Pellentesque fermentum et turpis ac feugiat. Suspendisse a purus commodo, adipiscing ligula ut, gravida leo. Donec blandit odio eget volutpat condimentum. Morbi eleifend sodales libero. Cras commodo sapien quis ante faucibus accumsan. Praesent dignissim nisl in egestas consectetur. Fusce eget quam dapibus, auctor eros dignissim, blandit mi. Pellentesque iaculis malesuada arcu, ultricies ullamcorper mauris. Vestibulum sit amet lorem nec nisl congue commodo id non tellus. Sed iaculis magna vitae nulla blandit, laoreet feugiat dui varius. Phasellus sed tincidunt orci, non pretium ante. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Curabitur facilisis eu mauris ac aliquet. Fusce feugiat accumsan diam, nec pellentesque ligula volutpat ac. Phasellus sit amet justo mattis, tincidunt nisi ut, viverra dui. Quisque at tellus ligula. Sed euismod nec lacus non pretium. Morbi iaculis sapien vel feugiat vehicula. Donec suscipit tellus vel lectus venenatis, vel varius dui eleifend. Curabitur porttitor pellentesque lacus eget interdum. Donec vitae dui a urna tincidunt rutrum sed at augue. Pellentesque vel pellentesque dui. Duis placerat libero arcu. Nullam bibendum consectetur arcu eget sagittis. In condimentum sagittis nunc auctor aliquet. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Aliquam erat volutpat. Proin euismod, leo sit amet pulvinar fermentum, eros quam volutpat libero, et vulputate diam dolor viverra nunc. Vestibulum vitae lacus tempor augue porta mollis in quis lacus. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Ut et imperdiet diam. Phasellus porttitor est libero, accumsan convallis velit faucibus nec. Sed euismod consequat orci, sed facilisis risus molestie ac. Sed sit amet dolor facilisis, tempus orci non, malesuada turpis. In lobortis tristique sodales. Aliquam posuere non risus vitae aliquam. Aliquam malesuada fringilla mauris, a egestas quam volutpat quis. Nam at ultrices risus. Nullam iaculis luctus massa. Sed feugiat tincidunt porta. Praesent iaculis lectus est, mattis mattis elit malesuada a. Aliquam pellentesque orci sed pellentesque venenatis. Nulla facilisi. Etiam dictum sollicitudin lorem, ac placerat lacus sodales quis. Nunc a erat ac enim congue egestas ac nec nibh. Aliquam eget porta ipsum. Vestibulum pharetra accumsan tincidunt. Integer quis tincidunt nisl, id vestibulum turpis. Aliquam erat volutpat. Suspendisse nunc leo, commodo in tristique eget, semper nec purus. Suspendisse cursus placerat iaculis. Vivamus vel sem nec enim vestibulum faucibus non eu lectus. Sed ac ligula in sapien pulvinar vestibulum. Nulla nec pulvinar est. Morbi egestas, justo a tempus ornare, lectus orci mattis diam, vitae varius magna mauris et tellus. Curabitur convallis pellentesque felis vitae semper. Suspendisse scelerisque enim id lectus convallis, et convallis leo commodo. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Nullam mauris ante, aliquet quis odio sed, porta tristique mauris. Duis eget interdum metus, vitae iaculis felis.' ]
	}, function(error, response) {
		if(error) {
			console.log(error, response);
		}
	})
})
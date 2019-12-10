var kafka = require('kafka-node');
var Consumer = kafka.Consumer,
    // The client specifies the ip of the Kafka producer and uses
    // the zookeeper port 2181
    client = new kafka.KafkaClient({kafkaHost: '10.3.100.196:9092'}),
    // The consumer object specifies the client and topic(s) it subscribes to
    consumer = new Consumer(
        client, [ { topic: 'bigindex3', partition: 0 } ], { autoCommit: false });

consumer.on('message', function (message) {
    // grab the main content from the Kafka message
    var data = JSON.parse(message.value);
    console.log(data);
});
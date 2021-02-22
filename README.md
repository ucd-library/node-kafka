# node-kafka
A wrapper around node-rdkafka implementing a standard way of using rdkafka

Please checkout [node-rdkafka](https://github.com/Blizzard/node-rdkafka) for base library documentation.


Sample Consumer
```javascript
const {Consumer, kafkaUtils} = require('@ucd-lib/node-kafka');

let consumer = new Consumer({
  'group.id': 'service-group-id',
  'metadata.broker.list': 'kafka:9092',
},{
  'auto.offset.reset' : 'earliest'
});

async function handleMessage(msg) {
  let id = kafkaUtils.getMsgId(msg);
  let payload = JSON.parse(msg.value); // assuming JSON payload
  
  // do stuff
}

(async function() {

  await consumer.connect();
  await kafka.utils.ensureTopic({
      topic : 'my-topic',
      num_partitions: 10,
      replication_factor: 1
    }, 
    {'metadata.broker.list': 'kafka:9092'}
  );

  // subscribe to front of committed offset
  await consumer.subscribe(['my-topic']);
  await consumer.consume(handleMessage);
})();
```

Sample Producer
Sample Consumer
```javascript
const {Producer, kafkaUtils} = require('@ucd-lib/node-kafka');

let producer = new Producer({
  'metadata.broker.list': 'kafka:9092'
});

async function handleMessage(msg) {
  let id = kafkaUtils.getMsgId(msg);
  let payload = JSON.parse(msg.value); // assuming JSON payload
  
  // do stuff
}

(async function() {

  await producer.connect();
  await kafka.utils.ensureTopic({
      topic : 'my-topic',
      num_partitions: 10,
      replication_factor: 1
    }, 
    {'metadata.broker.list': 'kafka:9092'}
  );

  // send message
  producer.produce({
    topic : 'my-topic',
    value : {
      // Your JSON message here
    },
    key : 'your-kafka-topic-key-here'
  });
})();
```
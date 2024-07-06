const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['192.168.0.120:9092']
});

const producer = kafka.producer();

const runProducer = async () => {
  await producer.connect();
  await producer.send({
    topic: 'my-topic',
    messages: [
      { key: 'key1', value: 'Hello KafkaJS user!' },
    ],
  });
  await producer.disconnect();
};

runProducer().catch(console.error);

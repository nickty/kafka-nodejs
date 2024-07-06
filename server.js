const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Kafka } = require('kafkajs');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: 'http://localhost:3000',
    methods: ['GET', 'POST'],
    allowedHeaders: ['Content-Type'],
    credentials: true,
  }
});

app.use(cors({
  origin: 'http://localhost:3000',
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type'],
  credentials: true,
}));

app.use(express.json());

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['192.168.0.120:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group' });

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const msg = {
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        value: message.value?.toString(),
      };
      console.log('Received message from Kafka:', msg); // Log message
      io.emit('message', msg);
    },
  });
};

runConsumer().catch(console.error);

io.on('connection', (socket) => {
  console.log('a user connected');
  socket.on('disconnect', () => {
    console.log('user disconnected');
  });
});

app.post('/send', async (req, res) => {
  const { key, value } = req.body;
  try {
    await producer.connect();
    await producer.send({
      topic: 'my-topic',
      messages: [
        { key, value },
      ],
    });
    await producer.disconnect();
    res.status(200).send('Message sent to Kafka');
  } catch (error) {
    console.error('Error sending message to Kafka:', error);
    res.status(500).send('Failed to send message to Kafka');
  }
});

server.listen(4000, () => {
  console.log('listening on *:4000');
});

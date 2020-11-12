const Kafka = require('node-rdkafka');
const bunyan = require('bunyan');
const yargs = require('yargs/yargs')
const logger = bunyan.createLogger({name: "myapp"});

const args = yargs(process.argv.slice(2))
  .options({
    'batchSize': {
      alias: 's',
      describe: 'Number of messages to produce in parallel',
      demandOption: true,
      type: 'number'
    },
    'batchInterval': {
      alias: 'i',
      describe: 'time in seconds to start next batch of messages',
      demandOption: true,
      type: 'number'
    },
    'topicName': {
      alias: 't',
      describe: 'topic name',
      demandOption: true,
      type: 'string'
    },
    'brokersList': {
      alias: 'l',
      describe: 'command separated broker list. e.g 192.168.0.1:9092,192.168.0.2:9092',
      demandOption: true,
      type: 'string'
    }
  })
  .help()
  .argv

const messagesBatch = Number(args.batchSize) 
const batchInterval = Number(args.batchInterval) * 1000
const topicName = args.topicName
const brokersList = args.brokersList
const partitionNum = 3
const replicationFactor = 3
let globalCount = 1
let batchMessageInterval

//==================== functions ====================//
function produceMessages(count) {
  let localCount = globalCount;
  for ( let i=globalCount; i < count + localCount; i++, globalCount++) {
    try {
      producer.produce(
        'ha-topic',
        null,
        Buffer.from(`Awesome message - ${i}`),
        'Stormwind',
        Date.now(),
      );
      logger.info(`Message ${i} sent successfully`);
      //producer.emit('event.stats');
      //producer.disconnect();
    } catch (err) {
      logger.error(`A problem occurred when sending our message - ${i} `, err);
    }
  }
}

function sendBatchMessagesAfterInterval(messageCount, interval) {
  batchMessageInterval = setInterval(() => {
    produceMessages(messageCount)
  }, batchInterval)
}

//==================== main ====================//
const client = Kafka.AdminClient.create({
  'client.id': 'first-client',
  'metadata.broker.list': brokersList
});

const producer = new Kafka.Producer({
  'metadata.broker.list': brokersList,
  'client.id': 'first-client',
  'dr_cb': true,
  'event_cb': true,
  'retry.backoff.ms': 200,
  'message.send.max.retries': 1,
  'socket.keepalive.enable': false,
  'queue.buffering.max.messages': 100,
  'queue.buffering.max.ms': 1000,
  'metadata.request.timeout.ms': 1000,
  'request.required.acks': 1
  //'internal.termination.signal': 127,
  //'statistics.interval.ms': 10000,
  //'debug': 'all'
});

client.deleteTopic(topicName, (err) => {
  if (err) {
    logger.warn('Error: deleting topic..', err.message)
  } else {
    logger.info(`Topic ${topicName} deleted successfully...`)
  }

  setTimeout(() => {
    client.createTopic({
      topic: topicName,
      num_partitions: partitionNum,
      replication_factor: replicationFactor
    }, function(err) {
      if (err) {
        logger.fatal(err.message)
        process.exit(4)
      }
      logger.info(`Topic ${topicName} created successfully....`)

      producer.connect({}, (err, data) => {
        if (err) {
          logger.fatal(err);
          process.exit(3);
        }
        logger.info("Producer connected Successfully...", data);
      });
    });
  }, 2000)
})

//==================== events ====================//
producer.on('ready', () => {
  setTimeout(() => {
    sendBatchMessagesAfterInterval(messagesBatch, batchInterval)
  }, 2000)
});

producer.on('event.error', function(err) {
  logger.error('Error from event.error', err);
  if (err.message === 'all broker connections are down') {
    clearInterval(batchMessageInterval)
    producer.disconnect()
    //process.exit(7)
  } else {
    producer.getMetadata({topic: topicName}, function(err, metadata) {
      if (err) {
        logger.error('Error getting metadata in event.error', err);
      } else {
        logger.info('Got metadata = ', metadata);
      }
    });
  }
})

producer.on('event', function(data) {
  logger.info('Error from event', data);
})

producer.on('event.log', function(data) {
  logger.info('Error from event.log', data);
})

producer.on('event.stats', function(stats) {
  logger.info('stats from event.stats ', stats);
})

producer.on('disconnected', (data) => {
  logger.info('Disconnected successfully ', data);
}
);

producer.setPollInterval(100);

```
$ node kafka_producer.js --help
Options:
      --version        Show version number                             [boolean]
  -s, --batchSize      Number of messages to produce in parallel
                                                             [number] [required]
  -i, --batchInterval  time in seconds to start next batch of messages
                                                             [number] [required]
  -t, --topicName      topic name                            [string] [required]
  -l, --brokersList    command separated broker list. e.g
                       192.168.0.1:9092,192.168.0.2:9092     [string] [required]
      --help           Show help                                       [boolean]
```

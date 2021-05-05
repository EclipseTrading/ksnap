# KSnap
Create and restore point-in-time snapshots of data stored in Apache Kafka.

## Usage
```
$ ksnap --help
usage: __main__.py [-h] -b BROKERS -t TOPICS -d DATA [--ignore-missing-topics]
                   [--threads THREADS] [--consumer-timeout CONSUMER_TIMEOUT]
                   [--kafka-library KAFKA_LIBRARY]
                   {backup,restore}

positional arguments:
  {backup,restore}

optional arguments:
  -h, --help            show this help message and exit
  -b BROKERS, --brokers BROKERS
                        Comma-separated list of brokers in format `host:port`.
  -t TOPICS, --topics TOPICS
                        Comma-separated list of topics
  -d DATA, --data DATA  Directory where this tool will store data or read from
  --ignore-missing-topics
                        Ignore missing topics in Kafka broker
  --threads THREADS     No of threads using for writing messages to Kafka
  --consumer-timeout CONSUMER_TIMEOUT
                        Timeout in seconds for consuming topics
  --kafka-library KAFKA_LIBRARY
                        Which kafka library to use for reading messages
```

Create point-in-time snapshot of data in topics Topic1 and Topic2 using:

```
$ mkdir backupDir
$ ksnap backup -b kafka1:9092,kafka2:9092 -t Topic1,Topic2 -d ./backupDir
```

Restore point-in-time snapshot for Topic1 using:

```
$ ksnap restore -b kafka1:9092,kafka2:9092 -t Topic1,Topic2 -d ./backupDir
```

## Install
You should use Python 3.6 or above
```
$ pip install git+https://github.com/EclipseTrading/ksnap.git
```

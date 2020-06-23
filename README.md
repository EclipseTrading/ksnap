# KSnap
Create and restore point-in-time snapshots of data stored in Apache Kafka.

## Usage
```
$ ksnap --help
usage: ksnap <Command> [-h|--help] [-b|--brokers "<value>"] -t|--topic
             "<value>" -d|--data "<value>"

             Create and restore point in time snapshots of Kafka data

Commands:

  backup   Create point-in-time snapshot of Kafka data
  restore  Restore point-in-time snapshot of Kafka data to cluster

Arguments:

  -h  --help     Print help information
  -b  --brokers  Comma-separated list of brokers in format `host:port'.
                 Default: localhost:9092
  -t  --topic    Comma-separated list of topics
  -d  --data     Directory where this tool will store data or read from
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
Get latest binary version for your architecture from [releases page](https://github.com/EclipseTrading/ksnap/releases/latest).
If your desired architecture is not available you can see below how to build from source.

## Build
You should use Go compiler 1.14 or newer

```
$ git clone https://github.com/EclipseTrading/ksnap.git
$ cd ksnap
$ go install .
```

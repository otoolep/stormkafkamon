stormkafkamon
=============

_stormkafkamon_ dumps the state of [Apache Storm](http://storm.apache.org/) [Kafka](http://kafka.apache.org/) consumer spouts, showing how far behind each is behind, relative to the Kafka partition it is consuming. Once running it presents output like so:
```
monitor.py --zserver zookeeper0 --topology  NoBoltsTopology --spoutroot testroot --friendly
+--------+-------+-----------+----------+-------------+--------+------------------------+-------------+----------+
| Broker | Topic | Partition | Earliest |    Latest   | Depth  |         Spout          |   Current   |  Delta   |
+--------+-------+-----------+----------+-------------+--------+------------------------+-------------+----------+
| kafka0 |  raw  |     0     |    0     | 12044626844 | 11.2GB | kafkaspout--61816062-0 | 12044445134 | 177.5KB  |
| kafka0 |  raw  |     1     |    0     | 12020309626 | 11.2GB | kafkaspout--61816062-0 | 12019988928 | 313.2KB  |
| kafka0 |  raw  |     2     |    0     | 12049894170 | 11.2GB | kafkaspout--61816062-0 | 12049312832 | 567.7KB  |
| kafka0 |  raw  |     3     |    0     | 12059079262 | 11.2GB | kafkaspout--61816062-0 | 12059079262 | 0.0bytes |
| kafka1 |  raw  |     0     |    0     | 12074374700 | 11.2GB | kafkaspout--61816062-0 | 12074200350 | 170.3KB  |
| kafka1 |  raw  |     1     |    0     | 12105806506 | 11.3GB | kafkaspout--61816062-0 | 12105806506 | 0.0bytes |
| kafka1 |  raw  |     2     |    0     | 12059575506 | 11.2GB | kafkaspout--61816062-0 | 12059258012 | 310.1KB  |
| kafka1 |  raw  |     3     |    0     | 12116313670 | 11.3GB | kafkaspout--61816062-0 | 12115976730 | 329.0KB  |
+--------+-------+-----------+----------+-------------+--------+------------------------+-------------+----------+

Number of brokers:       2
Number of partitions:    8
Total broker depth:      89.9GB
Total delta:             1.8MB
```
This tool could be used to perform simple monitoring of spout throughput.

Tested against Kafka [0.72](http://kafka.apache.org/downloads.html) and Storm 0.82 (along with associated Kafka spout from [storm-contrib](https://github.com/nathanmarz/storm-contrib)), running on Ubuntu 12.04.

## Requirements

After cloning, run `pip install stormkafkamon`, or just

```
pip install https://github.com/otoolep/stormkafkamon/zipball/master
```

## Program implementation

The code iterates through all Spout entries in Zookeeper, and retrieves all details. It then contacts each Kafka broker listed in those details, and queries for the earliest available offset, and latest, of each partition. This allows it to display the details shown in the example.

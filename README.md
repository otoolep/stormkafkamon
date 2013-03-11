stormkafkamon
=============

Dumps state of Storm Kafka consumer spouts, showing how far behind each is behind, relative to the Kafka partition it is consuming. Check the "example" file for some sample output. This tool could be used to perform simple monitoring of spout throughput.

Tested against Kafka 0.72 and Storm 0.82 (along with associated Kafka spout from storm-contrib).

Requirements:

- Kazoo Zookeeper client. Install via "pip install kazoo".

- PrettyTable

Download and install from: http://code.google.com/p/prettytable/

- kafka-python

Download and install from: https://github.com/mumrah/kafka-python

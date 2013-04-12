stormkafkamon
=============

Dumps state of Storm Kafka consumer spouts, showing how far behind each is behind, relative to the Kafka partition it is consuming. Check the "example" file for some sample output. This tool could be used to perform simple monitoring of spout throughput.

Tested against Kafka 0.72 and Storm 0.82 (along with associated Kafka spout from storm-contrib), running on Ubunutu 12.04.

Requirements:

After cloning, run `pip install stormkafkamon`, or just

```
pip install https://github.com/otoolep/stormkafkamon/zipball/master
```

Workflow:

The code iterates through all Spout entries in Zookeeper, and retrieves all details. It then contacts each Kafka broker listed in those details, and queries for the earliest available offset, and latest, of each partition. This allows it to display the details shown in the example.

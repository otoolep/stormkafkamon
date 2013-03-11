stormkafkamon
=============

Dumps state of Storm Kafka consumers, showing how far behind each is behind, relative to the Kafka partition it is consuming. For example:

monitor.py --zserver 50.17.158.34 --topology Topology --spoutroot /stormkafka --friendly
+--------+-------+-----------+------------+------------+--------------------------+-----------+-------+
| Broker | Topic | Partition |  Earliest  |   Latest   |          Spout           |  Current  | Delta |
+--------+-------+-----------+------------+------------+--------------------------+-----------+-------+
| kafka0 |  raw  |     0     | 2684740972 | 5992210270 | kafkaspout--2013009557-0 | 359775474 | 5.2MB |
| kafka1 |  raw  |     0     | 2684806902 | 7142647722 | kafkaspout--2013009557-0 | 355649146 | 6.3MB |
+--------+-------+-----------+------------+------------+--------------------------+-----------+-------+

Tested against Kafka 0.72 and Storm 0.82 (along with associated Kafka spout).

Requirements:

- Kazoo Zookeeper client. Install via pip install kazoo.

- PrettyTable

Download and install from: http://code.google.com/p/prettytable/

- kafka-python

Download and install from: https://github.com/mumrah/kafka-python

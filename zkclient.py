import simplejson as json
from collections import namedtuple

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

ZkKafkaBroker = namedtuple('ZkKafkaBroker', ['id', 'host', 'port'])
ZkKafkaSpout = namedtuple('ZkKafkaSpout', ['id', 'partitions'])
ZkKafkaTopic = namedtuple('ZkKafkaTopic', ['topic', 'broker', 'num_partitions'])

class ZkError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class ZkClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = KazooClient(hosts=':'.join([host, str(port)]))

    @classmethod
    def _zjoin(cls, e):
        return '/'.join(e)

    def brokers(self, broker_root='/brokers'):
        '''
        Returns a list of ZkKafkaBroker tuples, where each value is a
        ZkKafkaBroker.
        '''
        b = []
        id_root = self._zjoin([broker_root, 'ids'])

        self.client.start()
        try:
            for c in self.client.get_children(id_root):
                n = self.client.get(self._zjoin([id_root, c]))[0]
                b.append(ZkKafkaBroker(c, n.split(':')[1], n.split(':')[2]))
        except NoNodeError:
            raise ZkError('Broker nodes do not exist in Zookeeper')
        self.client.stop()
        return b

    def topics(self, broker_root='/brokers'):
        '''
        Returns a list of ZkKafkaTopic tuples, where each tuple represents
        a topic being stored in a broker.
        '''
        topics = []
        t_root = self._zjoin([broker_root, 'topics'])

        self.client.start()
        try:
            for t in self.client.get_children(t_root):
                for b in self.client.get_children(self._zjoin([t_root, t])):
                    n = self.client.get(self._zjoin([t_root, t, b]))[0]
                    topics.append(ZkKafkaTopic._make([t, b, n]))
        except NoNodeError:
            raise ZkError('Topic nodes do not exist in Zookeeper')
        self.client.stop()
        return topics

    def spouts(self, spout_root, topology):
        '''
        Returns a list of ZkKafkaSpout tuples, where each tuple represents
        a Storm Kafka Spout.
        '''
        s = []
        self.client.start()
        try:
            for c in self.client.get_children(spout_root):
                partitions = []
                for p in self.client.get_children(self._zjoin([spout_root, c])):
                    j = json.loads(self.client.get(self._zjoin([spout_root, c, p]))[0])
                    if j['topology']['name'] == topology:
                        partitions.append(j)
                s.append(ZkKafkaSpout._make([c, partitions]))
        except NoNodeError:
            raise ZkError('Kafka Spout nodes do not exist in Zookeeper')
        self.client.stop()
        return tuple(s)

#!/usr/bin/env python

import argparse
import sys
import simplejson as json
from collections import namedtuple

from kazoo.client import KazooClient

ZkKafkaBroker = namedtuple('ZkKafkaBroker', ['id', 'host', 'port'])
ZkKafkaSpout = namedtuple('ZkKafkaSpout', ['id', 'partitions'])

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
        Returns a list of ZkKafkaBroker tuples, where each tuple represents
        a broker.
        '''
        b = []
        id_root = broker_root + '/ids'

        self.client.start()
        for c in self.client.get_children(id_root):
            n = self.client.get(self._zjoin([id_root, c]))[0]
            b.append(ZkKafkaBroker._make(n.split(':')))
        self.client.stop()
        return tuple(b)

    def spouts(self, spout_root):
        '''
        Returns a list of ZkKafkaSpout tuples, where each tuple represents
        a Storm Kafka Spout.
        '''
        s = []
        self.client.start()
        for c in self.client.get_children(spout_root):
            partitions = []
            for p in self.client.get_children(self._zjoin([spout_root, c])):
                j = json.loads(self.client.get(self._zjoin([spout_root, c, p]))[0])
                partitions.append(j)
            s.append(ZkKafkaSpout._make([c, partitions]))
        self.client.stop()
        return tuple(s)

######################################################################

def read_args():
    parser = argparse.ArgumentParser(
        description='Show complete state of Storm-Kafka consumers')
    parser.add_argument('--zserver', default='localhost',
        help='Zookeeper host (default: localhost)')
    parser.add_argument('--zport', type=int, default=2181,
        help='Zookeeper port (default: 2181)')
    parser.add_argument('--topic', type=str, default='test',
        help='Kafka topic (default: test)')
    return parser.parse_args()

def main():
    options = read_args()

    zc = ZkClient(options.zserver, options.zport)

    for b in zc.brokers():
        print b

    for s in zc.spouts('/firestorm'):
        print s

if __name__ == '__main__':
    sys.exit(main())

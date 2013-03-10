#!/usr/bin/env python

import argparse
import sys
from collections import namedtuple

from kazoo.client import KazooClient

KafkaBroker = namedtuple('KafkaBroker', ['id', 'host', 'port'])

class ZkKafka:
    IDS_KEY = '/brokers/ids'

    @classmethod
    def id_path(cls, id):
        return '/'.join([cls.IDS_KEY, str(id)])

class ZkClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = KazooClient(hosts=':'.join([host, str(port)]))

    def brokers(self):
        '''
        Returns a list of KafkaBroker typles, where each tuple represents
        a broker.
        '''
        b = []
        self.client.start()
        for c in self.client.get_children(ZkKafka.IDS_KEY):
            n = self.client.get(ZkKafka.id_path(c))[0]
            b.append(KafkaBroker._make(n.split(':')))
        self.client.stop()
        return tuple(b)

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

if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python

import argparse
import sys
from zkclient import *

def read_args():
    parser = argparse.ArgumentParser(
        description='Show complete state of Storm-Kafka consumers')
    parser.add_argument('--zserver', default='localhost',
        help='Zookeeper host (default: localhost)')
    parser.add_argument('--zport', type=int, default=2181,
        help='Zookeeper port (default: 2181)')
    parser.add_argument('--topic', type=str, default='test',
        help='Kafka topic (default: test)')
    parser.add_argument('--topology', type=str, required=True,
        help='Storm Topology')
    return parser.parse_args()

def main():
    options = read_args()

    zc = ZkClient(options.zserver, options.zport)

    for b in zc.brokers():
        print b

    for s in zc.spouts('/firestorm', options.topology):
        print s

    for p in zc.partitions():
        print p

if __name__ == '__main__':
    sys.exit(main())

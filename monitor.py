#!/usr/bin/env python

import argparse
import sys
from prettytable import PrettyTable

from zkclient import *
from processor import process

def sizeof_fmt(num):
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

def null_fmt(num):
    return num

def display(partitions, friendly=False):
    if friendly:
        delta_label = 'Delta'
        fmt = sizeof_fmt
    else:
        delta_label = 'Delta (bytes)'
        fmt = null_fmt

    table = PrettyTable(['Broker', 'Topic', 'Partition', 'Earliest', 'Latest',
                       'Spout', 'Current', delta_label])
    table.align['broker'] = 'l'

    for p in partitions:
        table.add_row([p.broker, p.topic, p.partition, p.earliest, p.latest,
                       p.spout, p.current, fmt(p.latest - p.current)])
    print table

######################################################################

def true_or_false_option(option):
    if option == None:
        return False
    else:
        return True

def read_args():
    parser = argparse.ArgumentParser(
        description='Show complete state of Storm-Kafka consumers')
    parser.add_argument('--zserver', default='localhost',
        help='Zookeeper host (default: localhost)')
    parser.add_argument('--zport', type=int, default=2181,
        help='Zookeeper port (default: 2181)')
    parser.add_argument('--topology', type=str, required=True,
        help='Storm Topology')
    parser.add_argument('--spoutroot', type=str, required=True,
        help='Root path for Kafka Spout data in Zookeeper')
    parser.add_argument('--friendly', action='store_const', const=True,
                    help='Show friendlier data')
    return parser.parse_args()

def main():
    options = read_args()

    zc = ZkClient(options.zserver, options.zport)

    display(process(zc.spouts(options.spoutroot, options.topology)),
            true_or_false_option(options.friendly))

if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python

import argparse
import sys
from prettytable import PrettyTable
import requests
import simplejson as json

from zkclient import ZkClient, ZkError
from processor import process, ProcessorError

def sizeof_fmt(num):
    for x in [' bytes','KB','MB','GB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

def null_fmt(num):
    return num

def display(summary, friendly=False):
    if friendly:
        fmt = sizeof_fmt
    else:
        fmt = null_fmt

    table = PrettyTable(['Broker', 'Topic', 'Partition', 'Earliest', 'Latest',
                        'Depth', 'Spout', 'Current', 'Delta'])
    table.align['broker'] = 'l'

    for p in summary.partitions:
        table.add_row([p.broker, p.topic, p.partition, p.earliest, p.latest,
                      fmt(p.depth), p.spout, p.current, fmt(p.delta)])
    print table.get_string(sortby='Broker')
    print
    print 'Number of brokers:       %d' % summary.num_brokers
    print 'Number of partitions:    %d' % summary.num_partitions
    print 'Total broker depth:      %s' % fmt(summary.total_depth)
    print 'Total delta:             %s' % fmt(summary.total_delta)

def post_json(endpoint, zk_data):
    fields = ("broker", "topic", "partition", "earliest", "latest", "depth",
              "spout", "current", "delta")
    json_data = {"%s-%s" % (p.broker, p.partition):
                 {name: getattr(p, name) for name in fields}
                 for p in zk_data.partitions}
    total_fields = ('depth', 'delta')
    total = {fieldname:
             sum(getattr(p, fieldname) for p in zk_data.partitions)
             for fieldname in total_fields}
    total['partitions'] = len({p.partition for p in zk_data.partitions})
    total['brokers'] = len({p.broker for p in zk_data.partitions})
    json_data['total'] = total
    requests.post(endpoint, data=json.dumps(json_data))

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
    parser.add_argument('--postjson', type=str,
                    help='endpoint to post json data to')
    return parser.parse_args()

def main():
    options = read_args()

    zc = ZkClient(options.zserver, options.zport)

    try:
        zk_data = process(zc.spouts(options.spoutroot, options.topology))
        if options.postjson:
            post_json(options.postjson, zk_data)
        else:
            display(zk_data, true_or_false_option(options.friendly))
    except ZkError, e:
        print 'Failed to access Zookeeper: %s' % str(e)
        return 1
    except ProcessorError, e:
        print 'Failed to process: %s' % str(e)
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())

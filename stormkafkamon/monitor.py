#!/usr/bin/env python

import os
import argparse
import sys
from time import sleep, time
from collections import deque
from prettytable import PrettyTable

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

def display(snap, rates, friendly=False, eventsize=360):
    if friendly:
        fmt = sizeof_fmt
    else:
        fmt = null_fmt

    table = PrettyTable(['Partition', 'Earliest', 'Latest', 'Depth', 'Incoming (/sec)', 'Current', 'Delta', 'Outgoing (/sec)'])

    total_delta = 0
    total_depth = 0
    total_incoming = 0
    total_outgoing = 0
    for k, v in snap[1].items():
        table.add_row([k, v.earliest, v.latest, fmt(v.depth), fmt(rates[k][0]), v.current, fmt(v.delta), fmt(rates[k][1])])
        total_delta = total_delta + v.delta
        total_depth = total_depth + v.depth
	total_incoming = total_incoming + rates[k][0]
	total_outgoing = total_outgoing + rates[k][1]

    print table.get_string(sortby='Partition')
    print
    print 'Total depth: %s' % fmt(total_depth)
    print 'Total delta: %s' % fmt(total_delta)
    print 'Total incoming: %s/sec %d/sec' % (fmt(total_incoming), total_incoming/eventsize)
    print 'Total outgoing: %s/sec %d/sec' % (fmt(total_outgoing), total_outgoing/eventsize)

######################################################################
# Calculate rates from two snaps.

def kafka_rates(partitions, old_snap, new_snap):
    assert(old_snap[0] < new_snap[0])
    delta_t = new_snap[0] - old_snap[0]

    results = {}
    for p in partitions:
        # Set a tuple for this partition of incoming rate and outgoing rate.
        results[p] = ((new_snap[1][p].latest - old_snap[1][p].latest) / delta_t,
                      (new_snap[1][p].current - old_snap[1][p].current) / delta_t)
    return results

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
    parser.add_argument('--zretry', type=int, default=5,
        help='Zookeeper error retry period (default: 5)')
    parser.add_argument('--topology', type=str, required=True,
        help='Storm Topology')
    parser.add_argument('--spoutroot', type=str, required=True, metavar='ROOT',
        help='Root path for Kafka Spout data in Zookeeper')
    parser.add_argument('--friendly', action='store_const', const=True,
                    help='Show friendlier data')
    parser.add_argument('--numsnaps', type=int, default=10, metavar='N',
                    help='Number of snaps to keep (default: 10)')
    parser.add_argument('--snapinterval', type=int, default=1, metavar='S',
                    help='Seconds between snaps (default: 1)')
    parser.add_argument('--eventsize', type=int, default=360, metavar='M',
                    help='Nominal event size (default: 360)')
    return parser.parse_args()

def main():
    options = read_args()

    kafka_snaps = deque(maxlen=options.numsnaps)
    zc = ZkClient(options.zserver, options.zport)

    while True:
        try:
            kafka_snap = process(zc.spouts(options.spoutroot, options.topology))
        except ZkError, e:
            print 'Failed to access Zookeeper: %s' % str(e)
	    sleep(options.zretry)
	    continue
        except ProcessorError, e:
            print 'Failed to process: %s' % str(e)
	    sleep(options.zretry)
	    continue

        kafka_snaps.append((int(time()), kafka_snap))
        if len(kafka_snaps) > 1:
            os.system('clear')
            display(kafka_snaps[-1], kafka_rates(sorted(kafka_snap), kafka_snaps[0], kafka_snaps[-1]),
                    options.friendly, options.eventsize)

        sleep(options.snapinterval)

    return 0

if __name__ == '__main__':
    sys.exit(main())

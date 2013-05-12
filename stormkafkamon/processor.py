# Takes lists of objects returned by the zkclient module, and
# consolidates the information for display.

import logging

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger = logging.getLogger('kafka.codec').addHandler(NullHandler())

import struct
import socket
from collections import namedtuple
from kafka.client import KafkaClient, OffsetRequest

class ProcessorError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

PartitionState = namedtuple('PartitionState',
    [
        'topic',            # Topic
        'earliest',         # Earliest offset within partition on broker
        'latest',           # Current offset within partition on broker
        'depth',            # Depth of partition on broker.
        'spout',            # The Spout consuming this partition
        'current',          # Current offset for Spout
        'delta'             # Difference between latest and current
    ])
PartitionsSummary = namedtuple('PartitionsSummary',
    [
        'total_depth',      # Total queue depth.
        'total_delta',      # Total delta across all spout tasks.
        'num_partitions',   # Number of partitions.
        'num_brokers',      # Number of Kafka Brokers.
        'partitions'        # Tuple of PartitionStates
    ])

def process(spouts):
    '''
    List of dictionary objects. Structure of dic objects:

    key:    'broker:partition'
    value:  Corresponding PartitionState object.
    '''
    for s in spouts:
        states = {}
        for p in s.partitions:
            try:
                k = KafkaClient(p['broker']['host'], p['broker']['port'])
            except socket.gaierror, e:
                raise ProcessorError('Failed to contact Kafka broker %s (%s)' %
                                     (p['broker']['host'], str(e)))
            earliest_off = OffsetRequest(p['topic'], p['partition'], -2, 1)
            latest_off = OffsetRequest(p['topic'], p['partition'], -1, 1)

            earliest = k.get_offsets(earliest_off)[0]
            latest = k.get_offsets(latest_off)[0]
            current = p['offset']

            states[p['broker']['host'] + ':' + str(p['partition'])] = PartitionState._make([
                p['topic'],
                earliest,
                latest,
                latest - earliest,
                s.id,
                current,
                latest - current])

    return states

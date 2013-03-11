# Takes lists of objects returned by the zkclient module, and
# consolidates the information for display.

import struct
import socket
from collections import namedtuple
from kafka.client import KafkaClient, OffsetRequest

PartitionState = namedtuple('PartitionState',
    [
        'broker',       # Broker host
        'topic',        # Topic on broker
        'partition',    # The partition
        'earliest',     # Earliest offset within partition on broker
        'latest',       # Current offset within partition on broker
        'spout',        # The Spout consuming this partition
        'current',      # Current offset for Spout
        'delta'         # Difference between latest and earliest
    ])

def process(spouts):
    results = []
    for s in spouts:
        for p in s.partitions:
            k = KafkaClient(p['broker']['host'], p['broker']['port'])
            earliest_off = OffsetRequest(p['topic'], p['partition'], -2, 1)
            latest_off = OffsetRequest(p['topic'], p['partition'], -1, 1)

            earliest = k.get_offsets(earliest_off)[0]
            latest = k.get_offsets(latest_off)[0]
            current = p['offset']

            results.append(PartitionState._make([
                p['broker']['host'],
                p['topic'],
                p['partition'],
                earliest,
                latest,
                s.id,
                current,
                latest - current]))
    return results

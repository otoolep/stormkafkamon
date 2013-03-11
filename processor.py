# Takes lists of objects returned by the zkclient module, and
# consolidates the information for display.

import struct
import socket
from collections import namedtuple

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
OffsetRequest = namedtuple("OffsetRequest", ["topic", "partition", "time", "maxOffsets"])

# Error codes from Kafka.
error_codes = {
   -1: "UnknownError",
    0: None,
    1: "OffsetOutOfRange",
    2: "InvalidMessage",
    3: "WrongPartition",
    4: "InvalidFetchSize"
}

class KafkaException(Exception):
    def __init__(self, errorType):
        self.errorType = errorType
    def __str__(self):
        return str(self.errorType)

class KafkaClient(object):
    OFFSET_KEY       = 4

    def __init__(self, host, port, bufsize=1024):
        self.host = host
        self.port = port
        self.bufsize = bufsize
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))

    @classmethod
    def _length_prefix_message(cls, msg):
        """
        Prefix a message with its length as an int
        """
        return struct.pack('>i', len(msg)) + msg

    def _consume_response_iter(self):
        """
        This method handles the response header and error messages. It
        then returns an iterator for the chunks of the response
        """
        # Header -- 6 bytes gives us the length and error code.
        resp = self._sock.recv(6)
        if resp == "":
            raise Exception("Got no response from Kafka")
        (size, err) = struct.unpack('>iH', resp)

        # Handle error
        error = error_codes.get(err)
        if error is not None:
            raise KafkaException(error)

        # Response iterator
        total = 0
        while total < (size-2):
            resp = self._sock.recv(self.bufsize)
            if resp == "":
                raise Exception("Underflow")
            total += len(resp)
            yield resp

    def _consume_response(self):
        """
        Fully consume the response iterator
        """
        data = ""
        for chunk in self._consume_response_iter():
            data += chunk
        return data

    @classmethod
    def _encode_offset_request(cls, offsetRequest):
        (topic, partition, offset, maxOffsets) = offsetRequest
        req = struct.pack('>HH%dsiqi' % len(topic), KafkaClient.OFFSET_KEY,
                          len(topic), topic, partition, offset, maxOffsets)
        return req

    def _get_offsets(self, offsetRequest):
        """
        Get the offsets for a topic

        Params
        ======
        offsetRequest: OffsetRequest
        Returns
        =======
        offsets: tuple of offsets
        Wire Format
        ===========
        <OffsetResponse> ::= <num> [ <offsets> ]
        <num> ::= <int32>
        <offsets> ::= <offset> [ <offsets> ]
        <offset> ::= <int64>

        """
        req = self._length_prefix_message(self._encode_offset_request(offsetRequest))
        sent = self._sock.sendall(req)
        if sent == 0:
            raise RuntimeError("Kafka went away")

        data = self._consume_response()
        (num,) = struct.unpack('>i', data[0:4])
        offsets = struct.unpack('>%dq' % num, data[4:])
        return offsets

    def get_latest_offset(self, topic, partition):
        return self._get_offsets(OffsetRequest(topic, partition, -1, 1))[0]

    def get_earliest_offset(self, topic, partition):
        return self._get_offsets(OffsetRequest(topic, partition, -2, 1))[0]

def process(spouts):
    results = []
    for s in spouts:
        for p in s.partitions:
            k = KafkaClient(p['broker']['host'], p['broker']['port'])
            earliest = k.get_earliest_offset(p['topic'], p['partition'])
            latest = k.get_latest_offset(p['topic'], p['partition'])
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

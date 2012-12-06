import binascii
import logging
import struct
import time
import sys, traceback
from cStringIO import StringIO
from collections import namedtuple
from datetime import datetime
from functools import partial

__all__ = [
    'KafkaError',
    'ConnectionFailure',
    'OffsetOutOfRange',
    'InvalidMessageCode',
    'WrongPartitionCode',
    'InvalidFetchSizeCode',
    'UnknownError',
    'InvalidOffset',
    'PRODUCE_REQUEST',
    'FETCH_REQUEST',
    'OFFSETS_REQUEST',
    'LATEST_OFFSET',
    'EARLIEST_OFFSET',
    'Lengths',
    'ConsumerStats'
]

VERSION_0_7 = False

class KafkaError(Exception): pass
class ConnectionFailure(KafkaError): pass
class OffsetOutOfRange(KafkaError): pass
class InvalidMessageCode(KafkaError): pass
class WrongPartitionCode(KafkaError): pass
class InvalidFetchSizeCode(KafkaError): pass
class UnknownError(KafkaError): pass
class InvalidOffset(KafkaError): pass

error_codes = {
    1: OffsetOutOfRange,
    2: InvalidMessageCode,
    3: WrongPartitionCode,
    4: InvalidFetchSizeCode,
}

PRODUCE_REQUEST      = 0
FETCH_REQUEST        = 1
MULTIFETCH_REQUEST   = 2
MULTIPRODUCE_REQUEST = 3
OFFSETS_REQUEST      = 4

MAGIC_BYTE = 0

LATEST_OFFSET   = -1
EARLIEST_OFFSET = -2

kafka_log  = logging.getLogger('brod')

class Lengths(object):
    ERROR_CODE = 2
    RESPONSE_SIZE = 4
    REQUEST_TYPE = 2
    TOPIC_LENGTH = 2
    PARTITION = 4
    OFFSET = 8
    OFFSET_COUNT = 4
    MAX_NUM_OFFSETS = 4
    MAX_REQUEST_SIZE = 4
    TIME_VAL = 8
    MESSAGE_LENGTH = 4
    MAGIC = 1
    COMPRESSION = 1
    CHECKSUM = 4
    MESSAGE_HEADER = MESSAGE_LENGTH + MAGIC + CHECKSUM

class BrokerPartition(namedtuple('BrokerPartition', 
                                 'broker_id partition creator host port topic')):
    @property
    def id(self):
        return "{0.broker_id}-{0.partition}".format(self)

    @classmethod
    def from_zk(cls, broker_id, broker_string, topic, num_parts):
        """Generate a list of BrokerPartition objects based on various values
        taken from ZooKeeper.

        broker_id is this broker's ID to ZooKeeper. It's a simple integer, set
        as the "brokerid" param in Kafka's server config file. You can find a
        list of them by asking for the children of /brokers/ids in ZooKeeper.

        broker_string is found in ZooKeeper at /brokers/ids/{broker_id}
        The format of broker_string is assumed to be "creator:host:port",
        though the creator can have the host embedded in it because of the
        version of UUID that Kafka uses.

        num_parts is the number of partitions for that broker and is located at
        /brokers/topics/{topic}/{broker_id}
        """
        creator, host, port = broker_string.split(":")
        num_parts = int(num_parts)

        return [BrokerPartition(broker_id=int(broker_id), partition=i,
                                creator=creator, host=host, port=int(port), 
                                topic=topic) 
                for i in range(num_parts)]

class ConsumerStats(namedtuple('ConsumerStats',
                               'fetches bytes messages max_fetch')):

    def _human_bytes(self, bytes):
        bytes = float(bytes)
        TB, GB, MB, KB = 1024**4, 1024**3, 1024**2, 1024   
        if bytes >= TB:     return '%.2fTB' % (bytes / TB)
        elif bytes >= GB:   return '%.2fGB' % (bytes / GB)
        elif bytes >= MB:   return '%.2fMB' % (bytes / MB)
        elif bytes >= KB:   return '%.2fKB' % (bytes / KB)
        else:               return '%.2fB' % bytes

    def __str__(self):
        return ("ConsumerStats: fetches={0}, bytes={1}, messages={2}, max_fetch={3}"
                .format(self.fetches, self._human_bytes(self.bytes), 
                        self.messages, self.max_fetch))


class FetchResult(object):
    """A FetchResult is what's returned when we do a MULTIFETCH request. It 
    can contain an arbitrary number of message sets, which it'll eventually
    be able to query more intelligently than this. :-P

    This should eventually move to base and be returned in a multifetch()
    """

    def __init__(self, message_sets):
        self._message_sets = message_sets[:]

    def __iter__(self):
        return iter(self._message_sets)
    
    def __len__(self):
        return len(self._message_sets)

    def __getitem__(self, i):
        return self._message_sets[i]
    
    @property
    def broker_partitions(self):
        return [msg_set.broker_partition for msg_set in self]
    
    @property
    def num_messages(self):
        return sum(len(msg_set) for msg_set in self)
    
    @property
    def num_bytes(self):
        return sum(msg_set.size for msg_set in self)


class MessageSet(object):
    """A collection of messages and offsets returned from a request made to
    a single broker/topic/partition. Allows you to iterate via (offset, msg)
    tuples and grab origin information.

    ZK info might not be available if this came from a regular multifetch. This
    should be moved to base.
    """
    def __init__(self, broker_partition, start_offset, offsets_msgs):
        self._broker_partition = broker_partition
        self._start_offset = start_offset
        self._offsets_msgs = offsets_msgs[:]
    
    ################## Where did I come from? ##################
    @property
    def broker_partition(self):
        return self._broker_partition

    @property
    def topic(self):
        return self.broker_partition.topic

    ################## What do I have inside? ##################
    @property
    def offsets(self):
        return [offset for offset, msg in self]

    @property
    def messages(self):
        return [msg for offset, msg in self]

    @property
    def start_offset(self):
        return self.offsets[0] if self else None
    
    @property
    def end_offset(self):
        return self.offsets[-1] if self else None

    @property
    def next_offset(self):
        # FIXME FIXME FIXME: This calcuation should be done at a much deeper
        # level, or else this won't work with compressed messages, or be able
        # to detect the difference between 0.6 and 0.7 headers
        if not self:
            return self._start_offset # We didn't read anything

        MESSAGE_HEADER_SIZE = 10 if VERSION_0_7 else 9
        last_offset, last_msg = self._offsets_msgs[-1]
        next_offset = last_offset + len(last_msg) + MESSAGE_HEADER_SIZE
        return next_offset

    @property
    def size(self):
        return sum(len(msg) for msg in self.messages)

    def __iter__(self):
        return iter(self._offsets_msgs)
    
    def __len__(self):
        return len(self._offsets_msgs)

    def __cmp__(self, other):
        bp_cmp = cmp(self.broker_partition, other.broker_partition)
        if bp_cmp:
            return bp_cmp
        else:
            return cmp(self._offsets_msgs, other.offsets_msgs)

    def __unicode__(self):
        return "Broker Partition: {0}\nContents: {1}".format(self.broker_partition, self._offsets_msgs)

    ################## Parse from binary ##################
    @classmethod
    def parse(self, data_buff):
        pass
        # 
        # MIN_MSG_SIZE = Lengths.MESSAGE_LENGTH + Lengths.MAGIC + Lengths.CHECKSUM
# 
        # def parse_message(msg_len, msg_data):
        #     pass
# 
        # req_len, req_type, topic_len = struct.unpack(">IHH", data_buff.read(12))
        # topic = unicode(buffer.read(topic_len), encoding='utf-8')
# 
# 
        # # data_len = 
# 
        # message_buffer.read(Lengths.MESSAGE_LENGTH)
# 
        # messages = [parse_message(msg_data) for msg_data in data]
# 
        # message_buffer.read(Lengths.MESSAGE_LENGTH)
# 
# 
# 
        # raise NotImplementedError()



import binascii
import io
import socket
import struct

class Brod(object):
    PLACEHOLDER = 0
    REQ_SIZE_LEN = 4
    MAGIC_BYTE = 2

    def __init__(self, host=None, port=None, client_id=None):
        self.host = host or 'localhost'
        self.port = port or 9092
        self.client_id = client_id or 'brod'
        self._request_seq = 0

    def produce(self, required_acks, timeout, topic_name, partition,
                message_set):

        return self.produce_many(required_acks, timeout,
            [(topic_name, [(partition, message_set)])])

    def produce_many(self, required_acks, timeout, topics):
        buf = self._create_request(api_key=0, api_version=0)

        # produce request parameters
        buf.write(struct.pack('>hI', required_acks, timeout))

        # num topics
        buf.write(struct.pack('>I', len(topics)))

        for topic, partitions in topics:
            # topic
            topic_len = len(topic)
            buf.write(struct.pack('>H{0}s'.format(topic_len),
                topic_len, topic))

            # num partitions
            buf.write(struct.pack('>I', len(partitions)))
            for partition, message_set in partitions:

                # partition
                buf.write(struct.pack('>I', partition))

                # remember where the message set size should go to fill in later
                message_set_size_pos = buf.tell()
                buf.write(struct.pack('>I', self.PLACEHOLDER))

                # num messages
                start_message_set_pos = buf.tell()
                partition_key = None
                partition_key_len = -1
                # partition_key = 'blah'
                # partition_key_len = len(partition_key)
                attributes = 0
                o = 0
                crc_len = 4 # bytes
                message_size_len = 4 # bytes
                for message in message_set:
                    # Offset, doesn't really matter for produce
                    buf.write(struct.pack('>q', o))
                    o += 1

                    # message size
                    message_size_pos = buf.tell()
                    buf.write(struct.pack('>i', self.PLACEHOLDER))

                    # crc
                    crc_pos = buf.tell()
                    buf.write(struct.pack('>i', self.PLACEHOLDER))

                    # magic byte, attributes, key, val
                    if partition_key is None:
                        msg_w_header = struct.pack('>BBi{0}s'.format(
                                            len(message)),
                            self.MAGIC_BYTE, attributes, partition_key_len,
                            message)
                    else:
                        msg_w_header = struct.pack('>BBi{0}s{1}s'.format(
                                            partition_key_len, len(message)),
                            self.MAGIC_BYTE, attributes, partition_key_len,
                            partition_key, message)

                    buf.write(msg_w_header)
                    end_msg = buf.tell()

                    # Seek back to the message size position and write it
                    buf.seek(message_size_pos)
                    message_size = end_msg - crc_pos
                    buf.write(struct.pack('>i', message_size))
                    # Fill in the crc
                    crc = self._calculate_checksum(msg_w_header)
                    # print 'crc: {0}'.format(crc)
                    # print 'len: {0} ({1}:{2})'.format(message_size, crc_pos, end_msg)
                    # print repr(msg_w_header)
                    buf.write(struct.pack('>i', crc))        
                    buf.seek(end_msg)

                end_message_set_pos = buf.tell()
                buf.seek(message_set_size_pos)
                buf.write(struct.pack('>I', end_message_set_pos - start_message_set_pos))
                buf.seek(end_message_set_pos)

        request_size = self._close_request(buf)

        sock = socket.socket()
        try:
            sock.connect((self.host, self.port))

            # Write the request
            bytes_sent = 0
            while bytes_sent < request_size:
                buf.seek(bytes_sent)
                bytes_sent += sock.send(buf.read())
            # print 'Sent {0} of {1} bytes'.format(bytes_sent, len(encoded_request))

            # Read the response size
            resp_version, resp_correlation_id, response = self._read_response(sock)

            # num topics
            (num_topics,) = struct.unpack('>I', response.read(4))
            response_data = []
            for n in range(num_topics):
                (topic_name_len,) = struct.unpack('>h', response.read(2))
                (topic_name, num_partitions) = struct.unpack('>{0}sI'.format(topic_name_len),
                    response.read(topic_name_len + 4))

                partition_offsets = []
                for m in range(num_partitions):
                    (partition, error_code, offset) = struct.unpack('>Ihq', response.read(4 + 2 + 8))
                    partition_offsets.append((partition, error_code, offset))
                response_data.append((topic_name, partition_offsets))

        finally:
            sock.close()

        return (resp_version,
             resp_correlation_id,
             response_data)

    def fetch(self, replica_id, max_wait_time, min_bytes, topic_name,
              partition, fetch_offset, max_bytes):
        return self.fetch_many(replica_id, max_wait_time, min_bytes,
            [(topic_name, [(partition, fetch_offset, max_bytes)])])

    def fetch_many(self, replica_id, max_wait_time, min_bytes, topics):
        buf = self._create_request(api_key=1, api_version=1)

        # fetch parameters
        buf.write(struct.pack('>iII', replica_id, max_wait_time, min_bytes))

        # num topics
        buf.write(struct.pack('>I', len(topics)))

        for topic, partitions in topics:
            # topic
            topic_len = len(topic)
            buf.write(struct.pack('>H{0}s'.format(topic_len),
                topic_len, topic))

            # num partitions
            buf.write(struct.pack('>I', len(partitions)))
            for partition, fetch_offset, max_bytes in partitions:
                # partition
                buf.write(struct.pack('>IqI',
                    partition, fetch_offset, max_bytes))

        request_size = self._close_request(buf)

        sock = socket.socket()
        try:
            sock.connect((self.host, self.port))

            # Write the request
            bytes_sent = 0
            while bytes_sent < request_size:
                buf.seek(bytes_sent)
                chunk = buf.read()
                # print repr(chunk), bytes_sent
                bytes_sent += sock.send(chunk)

            # Read the response size
            resp_version, resp_correlation_id, response = self._read_response(sock)

            # num topics
            (num_topics,) = struct.unpack('>I', response.read(4))
            response_data = []
            for n in range(num_topics):
                (topic_name_len,) = struct.unpack('>h', response.read(2))
                (topic_name, num_partitions) = struct.unpack('>{0}sI'.format(topic_name_len), response.read(topic_name_len + 4))

                partition_messages = []
                for m in range(num_partitions):
                    (partition,
                     error_code,
                     fetched_offset,
                     highwater_mark_offset,
                     message_set_size) = struct.unpack('>IhqQi',
                        response.read(4 + 2 + 8 + 8 + 4))

                    messages = []
                    start_message_set_pos = response.tell()
                    while response.tell() - start_message_set_pos < message_set_size:
                        offset, message_size = struct.unpack('>qi',
                            response.read(8 + 4))
                        # print offset, message_size
                        header_len = 4 + 1 + 1 + 4

                        (crc,) = struct.unpack('>i', response.read(4))
                        msg_w_header = response.read(header_len - 4)
                        magic_byte, attributes, key_len = struct.unpack(
                            '>BBi', msg_w_header)
                        assert magic_byte == self.MAGIC_BYTE, (magic_byte, self.MAGIC_BYTE)

                        # read the partition key, if any
                        if key_len > 0:
                            header_len += key_len
                            partition_key = response.read(key_len)
                            msg_w_header += partition_key
                        else:
                            partition_key = None
                        message = response.read(message_size - header_len)

                        # check the crc
                        msg_w_header += message
                        actual_crc = self._calculate_checksum(msg_w_header)
                        assert crc == actual_crc, (crc, actual_crc)

                        messages.append((offset, message))
                    partition_messages.append((partition, error_code, fetched_offset, highwater_mark_offset, messages))
                response_data.append((topic_name, partition_messages))

        finally:
            sock.close()

        return resp_version, resp_correlation_id, response_data

    def _create_request(self, api_key, api_version):
        buf = io.BytesIO()
        self._request_seq += 1
        correlation_id = self._request_seq

        # generic request parameters
        buf.write(struct.pack('>IHHih{0}s'.format(len(self.client_id)),
            self.PLACEHOLDER, api_key, api_version, correlation_id,
            len(self.client_id), self.client_id))
        return buf

    def _close_request(self, buf):
        # Fill in the total request size
        end_pos = buf.tell()
        request_size = end_pos - self.REQ_SIZE_LEN
        buf.seek(0)
        buf.write(struct.pack('>I', request_size))
        buf.seek(0)
        return end_pos

    def _read_response(self, sock):
        response_size = sock.recv(self.REQ_SIZE_LEN)
        (size,) = struct.unpack('>i', response_size)

        # Read the response
        response = io.BytesIO()
        bytes_read = 0
        while bytes_read < size:
            response.write(sock.recv(size - bytes_read))
            bytes_read = response.tell()
        response.seek(0)

        # General response vals
        resp_version, resp_correlation_id = struct.unpack('>hi',
            response.read(2 + 4))
        return resp_version, resp_correlation_id, response

    @staticmethod
    def _calculate_checksum(val):
        return binascii.crc32(val)



# By David Ormsbee (dave@datadog.com):
class Partition(object):
    """This is deprectated, and should be rolled up into the higher level 
    Consumers.

    A higher level abstraction over the Kafka object to make dealing with
    Partitions a little easier. Currently only serves to read from a topic.
    
    This class has not been properly tested with the non-blocking KafkaTornado.
    """
    PollingStatus = namedtuple('PollingStatus', 
                               'start_offset next_offset last_offset_read ' +
                               'messages_read bytes_read num_fetches ' +
                               'polling_start_time seconds_slept')
    
    def __init__(self, kafka, topic, partition=None):
        self._kafka = kafka
        self._topic = topic
        self._partition = partition

    def earliest_offset(self):
        """Return the first offset we have a message for."""
        return self._kafka.offsets(self._topic, EARLIEST_OFFSET, max_offsets=1,
                                   partition=self._partition)[0]
    
    def latest_offset(self):
        """Return the latest offset we can request. Note that this is the offset
        *after* the last known message in the queue. The offset this method 
        returns will not have a message in it at the time you call it, but it's
        where the next message *will* be placed, whenever it arrives."""
        return self._kafka.offsets(self._topic, LATEST_OFFSET, max_offsets=1,
                                   partition=self._partition)[0]
    

    # FIXME DO: Put callback in
    # Partition should have it's own fetch() with the basic stuff pre-filled
    def poll(self, 
             offset=None,
             end_offset=None,
             poll_interval=1,
             max_size=None,
             include_corrupt=False,
             retry_limit=3):
        """Poll and iterate through messages from a Kafka queue.

        Params (all optional):
            offset:     Offset of the first message requested.
            end_offset: Offset of the last message requested. We will return 
                        the message that corresponds to end_offset, and then
                        stop.
            poll_interval: How many seconds to pause between polling
            max_size:   maximum size to read from the queue, in bytes
            include_corrupt: 
            
        
        This is a generator that will yield (status, messages) pairs, where
        status is a Partition.PollingStatus showing the work done to date by this
        Partition, and messages is a list of strs representing all available
        messages at this time for the topic and partition this Partition was
        initialized with.
        
        By default, the generator will pause for 1 second between polling for
        more messages.
        
        Example:
        
            dog_queue = Kafka().partition('good_dogs')
            for status, messages in dog_queue.poll(offset, poll_interval=5):
                for message in messages:
                    dog, bark = parse_barking(message)
                    print "{0} barked: {1}!".format(dog, bark)
                print "Count of barks received: {0}".format(status.messages_read)
                print "Total barking received: {0}".format(status.bytes_read)
        
        Note that this method assumes we can increment the offset by knowing the
        last read offset, the last read message size, and the header size. This
        will change if compression ever gets implemented and the header format
        changes: https://issues.apache.org/jira/browse/KAFKA-79
        """
        # Kafka msg headers are 9 bytes: 4=len(msg), 1=magic val, 4=CRC
        MESSAGE_HEADER_SIZE = 9

        # Init for first run
        first_loop = True
        start_offset = self.latest_offset() if offset is None else offset
        last_offset_read = None # The offset of the last message we returned
        messages_read = 0 # How many messages have we read from the stream?
        bytes_read = 0 # Total number of bytes read from the stream?
        num_fetches = 0 # Number of times we've called fetch()
        seconds_slept = 0
        polling_start_time = datetime.now()

        # Shorthand fetch call alias with everything filled in except offset
        # The return from a call to fetch is list of (offset, msg) tuples that 
        # look like: [(0, 'Rusty'), (14, 'Patty'), (28, 'Jack'), (41, 'Clyde')]
        fetch_messages = partial(self._kafka.fetch,
                                 self._topic,
                                 partition=self._partition,
                                 max_size=max_size,
                                 callback=None,
                                 include_corrupt=include_corrupt)
        retry_attempts = 0
        while True:
            if end_offset is not None and offset > end_offset:
                break
            try:
                msg_batch = fetch_messages(offset)
                retry_attempts = 0 # resets after every successful fetch
            except (ConnectionFailure, IOError) as ex:
                if retry_limit is not None and retry_attempts > retry_limit:
                    kafka_log.exception(ex)
                    raise
                else:
                    time.sleep(poll_interval)
                    retry_attempts += 1
                    # kafka_log.exception(ex)
                    kafka_log.error("Retry #{0} for fetch of topic {1}, offset {2}"
                                    .format(retry_attempts, self._topic, offset))
                    continue
            except OffsetOutOfRange:
                # Catching and re-raising this with more helpful info.
                raise OffsetOutOfRange(("Offset {offset} is out of range for " +
                                       "topic {topic}, partition {partition} " + 
                                       "(earliest: {earliest}, latest: {latest})")
                                       .format(offset=offset,
                                               topic=self._topic,
                                               partition=self._partition,
                                               earliest=self.earliest_offset(),
                                               latest=self.latest_offset()))

            # Filter out the messages that are past our end_offset
            if end_offset is not None:
               msg_batch = [(msg_offset, msg) for msg_offset, msg in msg_batch
                            if msg_offset <= end_offset]

            # For the first loop only, if nothing came back from the batch, make
            # sure that the offset we're asking for is a valid one. Right
            # now, Kafka.fetch() will just silently return an empty list if an
            # invalid-but-in-plausible-range offset is requested. We assume that
            # if we get past the first loop, we're ok, because we don't want to
            # constantly call earliest/latest_offset() (they're network calls)
            if first_loop and not msg_batch:
                # If we're not at the latest available offset, then a call to 
                # fetch should return us something if it's valid. We have to 
                # make another fetch here because there's a chance 
                # latest_offset() could have moved since the last fetch.
                if self.earliest_offset() <= offset < self.latest_offset() and \
                   not fetch_messages(offset):
                    raise InvalidOffset("No message at offset {0}".format(offset))
            first_loop = False

            # Our typical processing...
            messages = [msg for msg_offset, msg in msg_batch]
            messages_read += len(messages)
            bytes_read += sum(len(msg) for msg in messages)
            num_fetches += 1

            if msg_batch:
                last_offset_read, last_message_read = msg_batch[-1]
                offset = last_offset_read + len(last_message_read) + \
                         MESSAGE_HEADER_SIZE

            status = Partition.PollingStatus(start_offset=start_offset,
                                             next_offset=offset,
                                             last_offset_read=last_offset_read,
                                             messages_read=messages_read,
                                             bytes_read=bytes_read,
                                             num_fetches=num_fetches,
                                             polling_start_time=polling_start_time,
                                             seconds_slept=seconds_slept)
        
            yield status, messages # messages is a list of strs
        
            # We keep grabbing as often as we can until we run out, after which
            # we start sleeping between calls until we see more.
            if poll_interval and not messages:
                time.sleep(poll_interval)
                seconds_slept += poll_interval


if __name__ == '__main__':
    brod = Brod()
    topic = 'test'
    partition = 0
    input_messages = ['yo', 'sup']
    _, _, produce_data = brod.produce(1, 5, topic, partition, input_messages)
    _, partition_offsets = produce_data[0]
    partition, error_code, offset = partition_offsets[0]

    _, _, fetch_data = brod.fetch(-1, 1000, 1, topic, partition, offset, 1024 * 1024)
    topic, partition_messages = fetch_data[0]
    partition, error_code, fetched_offset, highwater_mark_offset, messages = partition_messages[0]

    assert input_messages == [m for o, m in messages], (input_messages, messages)
    print messages






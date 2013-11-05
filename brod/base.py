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




class BaseKafka(object):
    MAX_RETRY = 3
    DEFAULT_MAX_SIZE = 1024 * 1024
    
    def __init__(self, host=None, port=None, max_size=None, 
            include_corrupt=False):
        self.host   = host or 'localhost'
        self.port   = port or 9092
        self.max_size = max_size or self.DEFAULT_MAX_SIZE
        self.include_corrupt = include_corrupt
    
    # Public API
    
    def produce(self, topic, messages, partition=None, callback=None):
        
        # Clean up the input parameters
        partition = partition or 0
        topic = topic.encode('utf-8')
        if isinstance(messages, unicode):
            messages = [messages.encode('utf-8')]
        elif isinstance(messages, str):
            messages = [messages]
        
        # Encode the request
        request = self._produce_request(topic, messages, partition)
        
        # Send the request
        return self._write(request, callback)
    
    def fetch(self, topic, offset, partition=None, max_size=None,
              callback=None, include_corrupt=False, min_size=None,
              fetch_step=None):
        """ Fetch messages from a kafka queue
            
            This will sequentially read and return all available messages 
            starting at the specified offset and adding up to max_size bytes.
            
            Params:
                topic:      kafka topic to read from
                offset:     offset of the first message requested
                partition:  topic partition to read from (optional)
                max_size:   maximum size to read from the queue,
                            in bytes (optional)
                min_size:   minimum size to read from the queue. if min_size and
                            fetch_step are defined, then we'll fetch sizes from
                            min_size to max_size until we have a result.
                fetch_step: the step increase for each fetch to the queue. only
                            applies if both a min_size and max_size are set.

            Returns:
                a list: [(offset, message), ]
        """
        if min_size and max_size and fetch_step:
            fetch_sizes = xrange(min_size, max_size, fetch_step)
        else:
            fetch_sizes = [max_size or self.max_size]

        # Clean up the input parameters
        topic = topic.encode('utf-8')
        partition = partition or 0

        for fetch_size in fetch_sizes:
            # Encode the request
            fetch_request_size, fetch_request = self._fetch_request(topic,
                offset, partition, fetch_size)

            # Send the request. The logic for handling the response
            # is in _read_fetch_response().
            try:
                result = self._write(
                    fetch_request_size,
                    partial(self._wrote_request_size,
                            fetch_request,
                            partial(self._read_fetch_response,
                                    callback,
                                    offset,
                                    include_corrupt
                                    )))
            except IOError as io_err:
                kafka_log.exception(io_err)
                raise ConnectionFailure("Fetch failure because of: {0}".format(io_err))

            if result:
                return result

        return result

    def offsets(self, topic, time_val, max_offsets, partition=None, callback=None):
        
        # Clean up the input parameters
        partition = partition or 0
        
        # Encode the request
        request_size, request = self._offsets_request(topic, time_val, 
            max_offsets, partition)
        
        # Send the request. The logic for handling the response 
        # is in _read_offset_response().
        
        return self._write(request_size, 
            partial(self._wrote_request_size, request, 
                partial(self._read_offset_response, callback)))

    def earliest_offset(self, topic, partition):
        """Return the first offset we have a message for."""
        return self.offsets(topic, EARLIEST_OFFSET, max_offsets=1, partition=partition)[0]
    
    def latest_offset(self, topic, partition):
        """Return the latest offset we can request. Note that this is the offset
        *after* the last known message in the queue. The offset this method 
        returns will not have a message in it at the time you call it, but it's
        where the next message *will* be placed, whenever it arrives."""
        return self.offsets(topic, LATEST_OFFSET, max_offsets=1, partition=partition)[0]
    
    # Helper methods
    
    @staticmethod
    def compute_checksum(value):
        return binascii.crc32(value)

    # Private methods

    # Response decoding methods
    
    def _read_fetch_response(self, callback, start_offset, include_corrupt, 
            message_buffer):
        if message_buffer:
            messages = list(self._parse_message_set(
                start_offset, message_buffer, include_corrupt)
            )
        else:
            messages = []

        if callback:
            return callback(messages)
        else:
            return messages

    def _parse_message_set(self, start_offset, message_buffer, 
            include_corrupt=False):
        offset = start_offset
        
        try:
            has_more = True
            while has_more:
                offset = start_offset + message_buffer.tell() - Lengths.ERROR_CODE
                
                # Parse the message length (uint:4)
                raw_message_length = message_buffer.read(Lengths.MESSAGE_LENGTH)
                
                if raw_message_length == '':
                    break
                elif len(raw_message_length) < Lengths.MESSAGE_LENGTH:
                    kafka_log.error('Unexpected end of message set. Expected {0} bytes for message length, only read {1}'.format(Lengths.MESSAGE_LENGTH, len(raw_message_length)))
                    break
                
                message_length = struct.unpack('>I', 
                    raw_message_length)[0]
                
                # Parse the magic byte (int:1)
                raw_magic = message_buffer.read(Lengths.MAGIC)
                if len(raw_magic) < Lengths.MAGIC:
                    kafka_log.error('Unexpected end of message set. Expected {0} bytes for magic byte, only read{1}'.format(Lengths.MAGIC, len(raw_magic)))
                    break
                
                magic = struct.unpack('>B', raw_magic)[0]
                if magic == 1:
                    compression = message_buffer.read(Lengths.COMPRESSION)
                    # We don't do anything with this at the moment.
                
                # Parse the checksum (int:4)
                raw_checksum = message_buffer.read(Lengths.CHECKSUM)
                if len(raw_checksum) < Lengths.CHECKSUM:
                    kafka_log.error('Unexpected end of message set. Expected {0} bytes for checksum, only read {1}'.format(Lengths.CHECKSUM, len(raw_checksum)))
                    break
                    
                checksum = struct.unpack('>i', raw_checksum)[0]
                
                # Parse the payload (variable length string)
                payload_length = message_length - Lengths.MAGIC - Lengths.CHECKSUM
                payload = message_buffer.read(payload_length)
                if len(payload) < payload_length and not self.include_corrupt:
                    # This is not an error - this happens everytime we reach
                    # the end of the read buffer without having parsed a complete msg
                    # kafka_log.error('Unexpected end of message set. Expected {0} bytes for payload, only read {1}'.format(payload_length, len(payload)))
                    break
                
                actual_checksum = self.compute_checksum(payload)
                if magic != MAGIC_BYTE:
                    kafka_log.error('Unexpected magic byte: {0} (expecting {1})'.format(magic, MAGIC_BYTE))
                    corrupt = True

                elif checksum != actual_checksum:
                    kafka_log.error('Checksum failure at offset {0}'.format(offset))
                    corrupt = True
                else:
                    corrupt = False

                if include_corrupt:
                    # kafka_log.debug('message {0}: (offset: {1}, {2} bytes, corrupt: {3})'.format(payload, offset, message_length, corrupt))
                    yield offset, payload, corrupt
                else:
                    # kafka_log.debug('message {0}: (offset: {1}, {2} bytes)'.format(payload, offset, message_length))
                    yield offset, payload
        except:
            kafka_log.error("Unexpected error:{0}".format(sys.exc_info()[0]))
        finally:
            message_buffer.close()

    def _read_offset_response(self, callback, data):
        # The number of offsets received (uint:4)
        raw_offset_count = data.read(Lengths.OFFSET_COUNT)
        offset_count = struct.unpack('>L', raw_offset_count)[0]

        offsets = []
        has_more = True
        for i in range(offset_count):
            raw_offset = data.read(Lengths.OFFSET)
            offset = struct.unpack('>Q', raw_offset)[0]
            offsets.append(offset)

        #assert data.getvalue() == '', 'Some leftover data in offset response buffer: {0}'.format(data.getvalue())
        kafka_log.debug('Received {0} offsets: {1}'.format(offset_count, len(offsets)))

        if callback:
            return callback(offsets)
        else:
            return offsets
    
    # Request encoding methods
    
    def _produce_request(self, topic, messages, partition):
        message_set_buffer = StringIO()

        for message in messages:
            # <<int:1, int:4, str>>
            encoded_message = struct.pack('>Bi{0}s'.format(len(message)), 
                MAGIC_BYTE, 
                self.compute_checksum(message), 
                message
            )
            message_size = len(encoded_message)
            bin_format = '>i{0}s'.format(message_size)
            message_set_buffer.write(struct.pack(bin_format, message_size, 
                encoded_message))

        message_set = message_set_buffer.getvalue()

        # create the request <<unit:4, uint:2, uint:2, str, uint:4, uint:4, str>>>
        request = (
            PRODUCE_REQUEST,
            len(topic),
            topic,
            partition,
            len(message_set),
            message_set
        )
        data = struct.pack('>HH{0}sII{1}s'.format(len(topic), len(message_set)),
            *request
        )
        request_size = len(data)
        bin_format = '<<uint:4, uint:2, uint:2, str:{0}, uint:4, uint:4, str:{1}>>'.format(len(topic), len(message_set))
        kafka_log.debug('produce request: {0} in format {1} ({2} bytes)'.format(request, bin_format, request_size))
        return struct.pack('>I{0}s'.format(request_size), request_size, data)
    
    def _fetch_request(self, topic, offset, partition, max_size):
        # Build fetch request request
        topic_length = len(topic)
        request_size = sum([
            Lengths.REQUEST_TYPE,
            Lengths.TOPIC_LENGTH, # length of the topic length
            topic_length,
            Lengths.PARTITION,
            Lengths.OFFSET,
            Lengths.MAX_REQUEST_SIZE
        ])
        request = (
            FETCH_REQUEST, 
            topic_length, 
            topic, 
            partition, 
            offset, 
            max_size
        )

        # Send the fetch request
        bin_format = '<<uint:4, uint:2, uint:2, str:{0}, uint:4, uint:8, uint:4>>'.format(topic_length)
        # kafka_log.info('fetch request: {0} in format {1} ({2} bytes)'.format(request, bin_format, request_size))
        
        bin_request_size = struct.pack('>I', request_size)
        bin_request = struct.pack('>HH%dsIQI' % topic_length, *request)
        return bin_request_size, bin_request
    
    def _offsets_request(self, topic, time_val, max_offsets, partition):
        offsets_request_size = sum([
            Lengths.REQUEST_TYPE,
            Lengths.TOPIC_LENGTH,
            len(topic),
            Lengths.PARTITION,
            Lengths.TIME_VAL,
            Lengths.MAX_NUM_OFFSETS,
        ])
        
        offsets_request = (
            OFFSETS_REQUEST, 
            len(topic), 
            topic, 
            partition, 
            time_val, 
            max_offsets
        )
        
        bin_format = '<<uint:4, uint:2, uint:2, str:{0}, uint:4, int:8, uint:4>>'.format(len(topic))
        # kafka_log.debug('Fetching offsets for {0}-{1}, time: {2}, max_offsets: {3} in format {5} ({4} bytes)'.format(topic, partition, time_val, max_offsets, offsets_request_size, bin_format))

        bin_request_size = struct.pack('>I', offsets_request_size)
        bin_request = struct.pack('>HH{0}sIqI'.format(len(topic)), 
            *offsets_request)

        return bin_request_size, bin_request

    # Request/response protocol
    def _wrote_request_size(self, request, callback):
        return self._write(request, partial(self._wrote_request, callback))

    def _wrote_request(self, callback):
        # Read the first 4 bytes, which is the response size (unsigned int)
        return self._read(Lengths.RESPONSE_SIZE, 
            partial(self._read_response_size, callback))

    def _read_response_size(self, callback, raw_buf_length):
        buf_length = struct.unpack('>I', raw_buf_length)[0]
        # kafka_log.info('response: {0} bytes'.format(buf_length))
        return self._read(buf_length, 
            partial(self._read_response, callback))
    
    def _read_response(self, callback, data):
        # Check if there is a non zero error code (2 byte unsigned int):
        response_buffer = StringIO(data)
        raw_error_code = response_buffer.read(Lengths.ERROR_CODE)
        error_code = struct.unpack('>H', raw_error_code)[0]
        if error_code != 0:
            raise error_codes.get(error_code, UnknownError)('Code: {0}'.format(error_code))
        else:
            return callback(response_buffer)
    
    # Socket management methods
    
    def _connect(self):
        raise NotImplementedError()

    def _disconnect(self):
        raise NotImplementedError()

    def _reconnect(self):
        self._disconnect()
        self._connect()

    def _read(self, length, callback=None):
        raise NotImplementedError()

    def _write(self, data, callback=None, retries=MAX_RETRY):
        raise NotImplementedError()

    def topic(self, topic, partition=None):
        """Return a Partition object that knows how to iterate through messages
        in a topic/partition."""
        return Partition(self, topic, partition)

    def partition(self, topic, partition=None):
        """Return a Partition object that knows how to iterate through messages
        in a topic/partition."""
        return Partition(self, topic, partition)


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
             min_size=None,
             max_size=None,
             fetch_step=None,
             include_corrupt=False,
             retry_limit=3):
        """Poll and iterate through messages from a Kafka queue.

        Params (all optional):
            offset:     Offset of the first message requested.
            end_offset: Offset of the last message requested. We will return 
                        the message that corresponds to end_offset, and then
                        stop.
            poll_interval: How many seconds to pause between polling
            min_size:   minimum size to read from the queue
            max_size:   maximum size to read from the queue, in bytes
            fetch_step: the step to increase the fetch size from min to max
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

        # Try fetching with a set of different max sizes until we return a
        # set of messages.
        if min_size and max_size and fetch_step:
            fetch_sizes = range(min_size, max_size, fetch_step)
        else:
            fetch_sizes = [max_size]

        # Shorthand fetch call alias with everything filled in except offset
        # The return from a call to fetch is list of (offset, msg) tuples that 
        # look like: [(0, 'Rusty'), (14, 'Patty'), (28, 'Jack'), (41, 'Clyde')]
        fetch_messages = partial(self._kafka.fetch,
                                 self._topic,
                                 partition=self._partition,
                                 min_size=min_size,
                                 max_size=max_size,
                                 fetch_step=fetch_step,
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








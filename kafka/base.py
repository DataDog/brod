import binascii
import logging
import struct
import time
from cStringIO import StringIO
from functools import partial

__all__ = [
    'KafkaError',
    'ConnectionFailure',
    'OffsetOutOfRange',
    'InvalidMessageCode',
    'WrongPartitionCode',
    'InvalidRetchSizeCode',
    'UnknownError',
    'PRODUCE_REQUEST',
    'FETCH_REQUEST',
    'OFFSETS_REQUEST',
    'LATEST_OFFSET',
    'EARLIEST_OFFSET',
    'Lengths',
]

class KafkaError(Exception): pass
class ConnectionFailure(KafkaError): pass
class OffsetOutOfRange(KafkaError): pass
class InvalidMessageCode(KafkaError): pass
class WrongPartitionCode(KafkaError): pass
class InvalidRetchSizeCode(KafkaError): pass
class UnknownError(KafkaError): pass

error_codes = {
    1: OffsetOutOfRange,
    2: InvalidMessageCode,
    3: WrongPartitionCode,
    4: InvalidRetchSizeCode,
}

PRODUCE_REQUEST      = 0
FETCH_REQUEST        = 1
MULTIFETCH_REQUEST   = 2
MULTIPRODUCE_REQUEST = 3
OFFSETS_REQUEST      = 4

MAGIC_BYTE = 0

LATEST_OFFSET   = -1
EARLIEST_OFFSET = -2

kafka_log  = logging.getLogger('kafka')

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
    CHECKSUM = 4
    MESSAGE_HEADER = MESSAGE_LENGTH + MAGIC + CHECKSUM

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
    
    def fetch(self, topic, offset, partition=None, max_size=None, callback=None, include_corrupt=False):
        """ Consume data from the topic queue. """
        
        # Clean up the input parameters
        topic = topic.encode('utf-8')
        partition = partition or 0
        max_size = max_size or self.max_size
        
        # Encode the request
        fetch_request_size, fetch_request = self._fetch_request(topic, offset, 
            partition, max_size)
        
        # Send the request. The logic for handling the response 
        # is in _read_fetch_response().
        return self._write(fetch_request_size, 
            partial(self._wrote_request_size, fetch_request, 
                partial(self._read_fetch_response, callback, offset, 
                    include_corrupt)))

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

        
    # Helper methods
    
    @staticmethod
    def compute_checksum(value):
        return binascii.crc32(value)

    # Private methods

    # Response decoding methods
    
    def _read_fetch_response(self, callback, start_offset, include_corrupt, 
            message_buffer):
        if message_buffer:
            messages = self._parse_message_set(start_offset, message_buffer, 
                include_corrupt)
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
                    kafka_log.error('Unexpected end of message set. Expected {0} bytes for payload, only read {1}'.format(payload_length, len(payload)))
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
                    kafka_log.debug('message {0}: (offset: {1}, {2} bytes, corrupt: {3})'.format(payload, offset, message_length, corrupt))
                    yield offset, payload, corrupt
                else:
                    kafka_log.debug('message {0}: (offset: {1}, {2} bytes)'.format(payload, offset, message_length))

                    yield offset, payload
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
        kafka_log.info('produce request: {0} in format {1} ({2} bytes)'.format(request, bin_format, request_size))
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
        kafka_log.info('fetch request: {0} in format {1} ({2} bytes)'.format(request, bin_format, request_size))
        
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
        kafka_log.debug('Fetching offsets for {0}-{1}, time: {2}, max_offsets: {3} in format {5} ({4} bytes)'.format(topic, partition, time_val, max_offsets, offsets_request_size, bin_format))

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
        kafka_log.info('response: {0} bytes'.format(buf_length))
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


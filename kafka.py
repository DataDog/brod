import array
import binascii
import logging
import socket
import struct
import time
from cStringIO import StringIO

class KafkaError(Exception): pass
class ConnectionFailure(KafkaError): pass
class OffsetOutOfRange(KafkaError): pass
class InvalidMessageCode(KafkaError): pass
class WrongPartitionCode(KafkaError): pass
class InvalidRetchSizeCode(KafkaError): pass
class UnknownError(KafkaError): pass
class InvalidMessage(KafkaError): pass

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
socket_log = logging.getLogger('kafka.socket')

class Kafka(object):
    MAX_RETRY = 3
    DEFAULT_MAX_SIZE = 1024 * 1024
    
    def __init__(self, host=None, port=None, max_size=None):
        self.host   = host or 'localhost'
        self.port   = port or 9092
        self.max_size = max_size or self.DEFAULT_MAX_SIZE

        self._socket = None
        self._overflow = ''
        
    def produce(self, topic, messages, partition=None):
        partition = partition or 0

        topic = topic.encode('utf-8')

        if isinstance(messages, unicode):
            messages = [messages.encode('utf-8')]
        elif isinstance(messages, str):
            messages = [messages]

        message_set_buffer = StringIO()

        for message in messages:
            # <MAGIC_BYTE: char> <CRC32: int> <PAYLOAD: bytes>
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

        # create the request as <REQUEST_SIZE: int>, <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER: bytes>
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
        return self._write(struct.pack('>I{0}s'.format(request_size), request_size, data))

    def fetch(self, topic, offset, partition=None, max_size=None):
        """ Consume data from the topic queue. """
        partition = partition or 0
        topic_length = len(topic)
        max_size = max_size or self.max_size

        # Build fetch request request
        request_size = 2 + 2 + topic_length + 4 + 8 + 4
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
        self._write(struct.pack('>I', request_size))
        self._write(struct.pack('>HH%dsIQI' % topic_length, *request))

        # Read the response
        raw_buf_length = self._read(4)
        buf_length = struct.unpack('>I', raw_buf_length)[0]
        data = self._read(buf_length)
        error_code = struct.unpack('>H', data[0:2])[0]
        kafka_log.info('fetch response: {0} bytes'.format(buf_length))

        if error_code != 0:
            raise error_codes.get(error_code, UnknownError)('Code: {0} (offset {1})'.format(error_code, self.offset))

        message_set = data[2:]

        messages  = []

        if message_set:
            processed = 0
            length    = len(message_set) - 4
            assert length > 0
            
            message_index = 0
            while (processed <= length):
                message_size_offset = processed + 4
                raw_message_size = message_set[processed:message_size_offset]
                message_size = struct.unpack('!I', raw_message_size)[0]

                assert message_size < len(message_set), message_size

                message_offset = message_size_offset + message_size
                raw_message = message_set[processed:message_offset]
                message = self.decode_message(raw_message)
                kafka_log.debug('message {1}: {2} ({0} bytes)'.format(message_size, message_index, message))

                offset_delta = 4 + message_size
                processed += offset_delta

                yield offset + processed, message
                message_index += 1

    def offsets(self, topic, time_val, max_offsets, partition=None):
        partition = partition or 0

        # Send the request
        offsets_request_size = 2 + 2 + len(topic) + 4 + 8 + 4
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

        self._write(struct.pack('>I', offsets_request_size))
        self._write(struct.pack('>HH{0}sIqI'.format(len(topic)), 
            *offsets_request))

        # Now read the response
        raw_buf_length = self._read(4)
        buf_length = struct.unpack('>I', raw_buf_length)[0]
        data = self._read(buf_length)

        error_code = struct.unpack('>H', data[0:2])[0]
        if error_code != 0:
            raise error_codes.get(error_code, UnknownError)('Code: {0}'.format(error_code))

        count = struct.unpack('>L', data[2:6])[0]

        offset_size = 8
        res = []
        pos = 6
        while pos < len(data):
            res.append(struct.unpack('>Q', data[pos:pos + offset_size])[0])
            pos += offset_size

        assert len(res) <= count, 'Received more offsets than expected ({0} > {1})'.format(len(res), count)
        kafka_log.debug('Received {0} offsets: {1}'.format(count, res))

        return res

    # Private methods
    
    def _connect(self):
        """ Connect to the Kafka server. """

        self._socket = socket.socket()
        try:
            self._socket.connect((self.host, self.port))
        except Exception, e:
            raise ConnectionFailure("Could not connect to kafka at {0}:{1}".format(self.host, self.port))

    def _reconnect(self):
        """ Reconnect to the Kafka server. """
        self._disconnect()
        self._connect()

    def _disconnect(self):
        """ Disconnect from the remote server & close the socket. """
        try:
            self._socket.close()
        except IOError:
            pass
        finally:
            self._socket = None

    def _read(self, length):
        """ Send a read request to the remote Kafka server. """

        if self._socket is None:
            self._connect()

        # Create a character array to act as the buffer.
        buf         = array.array('c', ' ' * length)
        read_length = 0
    
        read_data_buf = StringIO()
    
        try:
            socket_log.debug('recv: expected {0} bytes'.format(length))
            while read_length < length:
                chunk_size = self._socket.recv_into(buf, length)
                chunk = buf.tostring()
                read_data_buf.write(chunk[0:chunk_size])
                read_length += chunk_size
                socket_log.debug('recv: {0} ({1} bytes)'.format(repr(chunk), 
                    chunk_size))

        except errno.EAGAIN:
            self.disconnect()
            raise IOError("Timeout reading from the socket.")
        else:
            read_data = read_data_buf.getvalue()
            socket_log.info('recv: {0} bytes total'.format(len(read_data)))
            output = self._overflow + read_data[0:length]
            self._overflow = read_data[length:]
      
            return output


    def _write(self, data, retries=MAX_RETRY):
        """ Write `data` to the remote Kafka server. """

        if self._socket is None:
            self._connect()

        wrote_length = 0

        try:
            write_length = len(data)
            wrote_length = 0

            while write_length > wrote_length:
                socket_log.info('send: {0}'.format(repr(data)))
                wrote_length += self._socket.send(data)

        except (errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED):
            # Retry once.
            self._reconnect()
            if retries > 0:
                return self._write(data, retries - 1)
            else:
                raise MaxRetries()
        else:
            return wrote_length

    @staticmethod
    def compute_checksum(value):
        return binascii.crc32(value)
    
    @classmethod
    def decode_message(cls, encoded_message):
        # A message. The format of an N byte message is the following:
        # 1 byte "magic" identifier to allow format changes
        # 4 byte CRC32 of the payload
        # N - 5 byte payload
        size     = struct.unpack('>I', encoded_message[0:4])[0]
        magic    = struct.unpack('>B', encoded_message[4:5])[0]
        checksum = struct.unpack('>i', encoded_message[5:9])[0]
        payload  = encoded_message[9:9 + size]
        
        actual_checksum = cls.compute_checksum(payload)
        assert checksum == actual_checksum, '{0} != {1}'.format(checksum, 
            actual_checksum)
        
        return payload




import logging
import struct
import time

import kafka.io
import kafka.request_type

class ConsumerError(Exception): pass
class OffsetOutOfRange(ConsumerError): pass
class InvalidMessageCode(ConsumerError): pass
class WrongPartitionCode(ConsumerError): pass
class InvalidRetchSizeCode(ConsumerError): pass
class UnknownError(ConsumerError): pass
class NoData(ConsumerError): pass
class InvalidMessage(ConsumerError): pass

error_codes = {
    1: OffsetOutOfRange,
    2: InvalidMessageCode,
    3: WrongPartitionCode,
    4: InvalidRetchSizeCode,
}

log = logging.getLogger('kafka.consumer')

class Consumer(kafka.io.IO):

  CONSUME_REQUEST_TYPE = kafka.request_type.FETCH
  MAX_SIZE = 1024 * 1024
  DEFAULT_POLLING_INTERVAL = 2 # seconds.
  MAX_OFFSETS = 100

  def __init__(self, topic, partition=0, host='localhost', port=9092, offset=0):
    kafka.io.IO.__init__(self, host, port)

    #: The topic queue to consume.
    self.topic        = topic
    #: The partition the topic queue is on.
    self.partition    = partition
    #: Offset in the Kafka queue in bytes?
    self.offset       = offset
    #: Maximum message size to consume.
    self.max_size     = self.MAX_SIZE
    self.request_type = self.CONSUME_REQUEST_TYPE
    self.polling      = self.DEFAULT_POLLING_INTERVAL
    self.connect()
    
    if self.offset < 0:
      self.send_offsets_request()
      self.offset = self.read_offsets_response()
      if offsets == []:
        raise Exception('No offsets for topic-partition')
  
  # REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
  def request_size(self):
    return 2 + 2 + len(self.topic) + 4 + 8 + 4
  
  def encode_request_size(self):
    return struct.pack('>I', self.request_size())

  def encode_request(self):
    length = len(self.topic)

    return struct.pack('>HH%dsIQI' % length, self.request_type, length, self.topic, self.partition, self.offset, self.max_size)

  def offsets_request_size(self):
    return 2 + 2 + len(self.topic) + 4 + 8 + 4
  

  def encode_offsets_request(self):
    return struct.pack('>I', self.offsets_request_size())
  
  def encode_offsets_request(self, topic):
    # create the request as <REQUEST_SIZE: int>, <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER: bytes>
    data = struct.pack('>H', kafka.request_type.OFFSETS) + \
           struct.pack('>H', len(topic)) + topic + \
           struct.pack('>I', partition) + \
           struct.pack('>Q', time) + \
           struct.pack('>I', max_offsets)
    return data    
  
  def consume(self):
    """ Consume data from the topic queue. """
    self.send_consume_request()
    try:
      return self.read_data_response()
    except NoData:
      return []
  
  def loop(self):
    """ Loop over incoming messages from the queue in a blocking fashion. Set `polling` for the check interval in seconds. """

    while True:
      messages = self.consume()
      for message in messages:
        if not message.is_valid():
          raise InvalidMessage('CRC mismatch')
        else:
          yield message
      
      time.sleep(self.polling)

  def send_consume_request(self):
    self.write(self.encode_request_size())
    self.write(self.encode_request())

  def read_data_response(self):
    raw_buf_length = self.read(4)
    buf_length = struct.unpack('>I', raw_buf_length)[0]
    data = self.read(buf_length)
    error_code = struct.unpack('>H', data[0:2])[0]
    
    if error_code != 0:
        raise error_codes.get(error_code, UnknownError)('Code: {0} (offset {1})'.format(error_code, self.offset))
    
    message_set = data[2:]
    
    if message_set:
        log.debug('buf_length: {0}'.format(buf_length))
        messages  = []
        processed = 0
        length    = len(message_set) - 4
        assert length > 0
        log.debug('message_set_size: {0}'.format(length))
        
        while (processed <= length):
          message_size_offset = processed + 4
          raw_message_size = message_set[processed:message_size_offset]
          message_size = struct.unpack('!I', raw_message_size)[0]
          
          log.debug('message_size: {0}'.format(message_size))
          assert message_size < len(message_set), message_size
          
          message_offset = message_size_offset + message_size
          raw_message = message_set[processed:message_offset]
          message = kafka.message.parse_from(raw_message)
          messages.append(message)
          processed += 4 + message_size

        self.offset += processed
        log.debug('New offset: {0}'.format(self.offset))
        return messages
    else:
        raise NoData()

  def read_offsets_response(self):
    buf_length = struct.unpack('>I', self.read(4))[0]
    data = self.read(buf_length)[2:]
    
    pos = 0
    error_code = struct.unpack('>H', data[pos,2][0])
    if error_code != 0:
      raise Exception ("Some Error")
    
    pos = pos + 2
    count = struct.unpack('>H', data[pos,4][0])
    pos = pos + 4
    
    res = []
    while ( pos != len(data) ):
      res << struct.unpack('>q', data[pos,8][0])
      pos = pos + 8
    
    # Start with a 2 byte offset
    return res
  
  def send_offsets_request(self):
    self.write(self.encode_offsets_request_size())
    self.write(self.encode_offsets_request(self.topic, self.partition, -2, self.MAX_OFFSETS ))
  

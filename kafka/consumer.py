import struct
import time

import kafka.io
import kafka.request_type

class Consumer(kafka.io.IO):

  CONSUME_REQUEST_TYPE = kafka.request_type.FETCH
  MAX_SIZE = 1024 * 1024
  DEFAULT_POLLING_INTERVAL = 2 # seconds.
  MAX_OFFSETS = 100

  def __init__(self, topic, partition=0, host='localhost', port=9092):
    kafka.io.IO.__init__(self, host, port)

    #: The topic queue to consume.
    self.topic        = topic
    #: The partition the topic queue is on.
    self.partition    = partition
    #: Offset in the Kafka queue in bytes?
    self.offset       = 0
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
    print self.topic
    print str(len(self.topic))
    return 2 + 2 + len(self.topic) + 4 + 8 + 4
  
  def encode_request_size(self):
    return struct.pack('>I', self.request_size())
  

  def encode_request(self):
    # create the request as <REQUEST_SIZE: int>, <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER: bytes>
    data = struct.pack('>H', self.request_type) + \
           struct.pack('>H', len(self.topic)) + self.topic + \
           struct.pack('>I', self.partition) + \
           struct.pack('>Q', self.offset) + \
           struct.pack('>I', self.max_size)
    return data    
  

  def offsets_request_size(self):
    print self.topic
    print str(len(self.topic))
    return 2 + 2 + len(self.topic) + 4 + 8 + 4
  

  def encode_offsets_request_size(self):
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

    return self.parse_message_set_from(self.read_data_response())
  
  def loop(self):
    """ Loop over incoming messages from the queue in a blocking fashion. Set `polling` for the check interval in seconds. """

    while True:
      messages = self.consume()
      for msg in messages:
        print "Message: ", msg.payload
      if messages and isinstance(messages, list) and len(messages) > 0:
        for message in messages:
          yield message

      time.sleep(self.polling)
  

  def read_data_response(self):
    buf_length = struct.unpack('>I', self.read(4))[0]

    # Start with a 2 byte offset
    return self.read(buf_length)[2:]
  
  def send_consume_request(self):
    x = self.encode_request_size()
    print "Encode Request Size: " , str(x)
    y = self.encode_request()
    print "Encode Request: " , str(y)
    self.write(x)
    self.write(y)
  
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
    x = self.encode_offsets_request_size()
    print "Encode Request Size: " , str(x)
    y = self.encode_offsets_request(self.topic, self.partition, -2, self.MAX_OFFSETS )
    print "Encode Request: " , str(y)
    self.write(x)
    self.write(y)
  

  def parse_message_set_from(self, data):
    messages  = []
    processed = 0
    length    = len(data) - 4

    while (processed <= length):
      message_size = struct.unpack('>I', data[processed:processed+4])[0]
      messages.append(kafka.message.parse_from(data[processed:processed + message_size + 4]))
      processed += 4 + message_size

    self.offset += processed

    return messages

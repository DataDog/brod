import contextlib
import itertools
import struct
import binascii

import kafka.io
import kafka.request_type

class Producer(kafka.io.IO):
  """ Class for sending data to a `Kafka <http://sna-projects.com/kafka/>`_ broker. """

  PRODUCE_REQUEST_ID = kafka.request_type.PRODUCE

  def __init__(self, topic, partition=0, host='localhost', port=9092):
    kafka.io.IO.__init__(self, host, port)
    self.topic     = topic
    self.partition = partition
    self.connect()

  def encode(self, message):
    """ Encode a :class:`Message` to binary form. """

    # <MAGIC_BYTE: char> <CRC32: int> <PAYLOAD: bytes>
    print message.payload
    print message.calculate_checksum()
    return struct.pack('>B', 0) + \
           struct.pack('>i', binascii.crc32(message.payload)) + \
           message.payload

  def encode_request(self, messages):
    """ Encode a sequence of :class:`Message <kafka.message>` objects for sending to the broker. """

    # encode messages as <LEN: int><MESSAGE_BYTES>
    encoded = [self.encode(message) for message in messages]
    message_set = ''.join([struct.pack('>I', len(m)) + m for m in encoded])
    
    # create the request as <REQUEST_SIZE: int>, <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER: bytes>
    data = struct.pack('>H', self.PRODUCE_REQUEST_ID) + \
           struct.pack('>H', len(self.topic)) + self.topic + \
           struct.pack('>I', self.partition) + \
           struct.pack('>I', len(message_set)) + message_set
    return struct.pack('>I', len(data)) + data
  
  def send(self, messages):
    """ Send a :class:`Message <kafka.message>` or a sequence of `Messages` to the Kafka server. """

    if isinstance(messages, kafka.message.Message):
      messages = [messages]

    return self.write(self.encode_request(messages))

  @contextlib.contextmanager
  def batch(self):
    """ Send messages with an implict `send`. """

    messages = []
    yield(messages)
    self.send(messages)
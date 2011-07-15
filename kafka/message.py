import struct
import binascii

def parse_from(binary):
  """ Turn a packed binary message as recieved from a Kafka broker into a :class:`Message`. """

  # A message. The format of an N byte message is the following:
  # 1 byte "magic" identifier to allow format changes
  # 4 byte CRC32 of the payload
  # N - 5 byte payload
  size     = struct.unpack('>I', binary[0:4])[0]
  magic    = struct.unpack('>B', binary[4:5])[0]
  checksum = struct.unpack('>I', binary[5:9])[0]
  payload  = binary[9:9+size]

  return Message(payload, magic, checksum)

class Message(object):
  
  MAGIC_IDENTIFIER_DEFAULT = 0

  def __init__(self, payload=None, magic=MAGIC_IDENTIFIER_DEFAULT, checksum=None):
    self.payload  = None
    self.magic    = magic
    self.checksum = checksum
    
    if payload is not None:
      self.payload = str(payload)
    
    if self.checksum is None:
      self.checksum = self.calculate_checksum()
  
  def calculate_checksum(self):
    return binascii.crc32(self.payload)
  
  def is_valid(self):
    return self.checksum == binascii.crc32(self.payload)
  

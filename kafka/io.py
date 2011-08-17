import array
import errno
import socket

class ConnectionFailure(Exception): pass

class IO(object):
  """ Base class for handling socket communication to the Kafka server. """

  def __init__(self, host='localhost', port=9092):
    self.socket = None

    #: Hostname to connect to.
    self.host   = host

    #: Port to connect to.
    self.port   = port

  def connect(self):
    """ Connect to the Kafka server. """

    self.socket = socket.socket()
    try:
        self.socket.connect((self.host, self.port))
    except Exception, e:
        raise ConnectionFailure("Could not connect to kafka at {0}:{1}".format(self.host, self.port))
        

  def reconnect(self):
    """ Reconnect to the Kafka server. """
    self.disconnect()
    self.connect()

  def disconnect(self):
    """ Disconnect from the remote server & close the socket. """
    try:
      self.socket.close()
    except IOError:
      pass
    finally:
      self.socket = None

  def read(self, length):
    """ Send a read request to the remote Kafka server. """

    try:
      return self.socket.recv(length)

    except errno.EAGAIN:
      self.disconnect()
      raise IOError, "Timeout reading from the socket."


  def write(self, data):
    """ Write `data` to the remote Kafka server. """

    if self.socket is None:
      self.reconnect()

    wrote_length = 0

    try:
      wrote_length = self.__write(data)

    except (errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED):
      # Retry once.
      self.reconnect()
      wrote_length = self.__write(data)

    finally:
      return wrote_length

  def __write(self, data):
    write_length = len(data)
    wrote_length = 0

    while write_length > wrote_length:
      wrote_length += self.socket.send(data)

    return wrote_length

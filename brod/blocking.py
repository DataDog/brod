import array
import errno
import socket

from brod.base import BaseKafka, logging, StringIO, ConnectionFailure
socket_log = logging.getLogger('brod.socket')

__all__ = [
    'Kafka',
]

class Kafka(BaseKafka):
    def __init__(self, *args, **kwargs):
        BaseKafka.__init__(self, *args, **kwargs)
        
        self._socket = None
        self._overflow = ''
        self.total_read = 0

    # Socket management methods
    
    def _connect(self):
        """ Connect to the Kafka server. """
        self._socket = socket.socket()
        try:
            self._socket.connect((self.host, self.port))
        except Exception, e:
            self._socket = None
            raise ConnectionFailure("Could not connect to kafka at {0}:{1}".format(self.host, self.port))

    def _disconnect(self):
        """ Disconnect from the remote server & close the socket. """
        try:
            self._socket.close()
        except IOError:
            pass
        finally:
            self._socket = None

    def _read(self, length, callback=None):
        """ Send a read request to the remote Kafka server. """
        
        if callback is None:
            callback = lambda v: v
        
        if self._socket is None:
            self._connect()

        read_length = 0
        read_data = ''
        
        try:
            # socket_log.debug('recv: expected {0} bytes'.format(length))
            while read_length < length:            
                chunk = self._socket.recv(length)
                read_length = read_length + len(chunk)
                read_data = read_data + chunk
                self.total_read += read_length
        except errno.EAGAIN:
            self._disconnect()
            raise IOError("Timeout reading from the socket.")
        except IOError:
            self._disconnect()
            raise
        else:
            # socket_log.info('recv: {0} bytes total'.format(len(read_data)))
            output = self._overflow + read_data[0:length]
            self._overflow = read_data[length:]
      
            return callback(output)

    def _write(self, data, callback=None, retries=BaseKafka.MAX_RETRY):
        """ Write `data` to the remote Kafka server. """
        
        if callback is None:
            callback = lambda: None
        
        if self._socket is None:
            self._connect()

        wrote_length = 0

        try:
            write_length = len(data)
            wrote_length = 0

            while write_length > wrote_length:
                # socket_log.info('send: {0}'.format(repr(data)))
                wrote_length += self._socket.send(data)

        except socket.error, e:
            if e.errno in [errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED]:
                # Retry once.
                self._reconnect()
                if retries > 0:
                    retries -= 1
                    socket_log.warn("Socket error (%s), reconnecting (%s retries left)" % (str(e), retries))
                    return self._write(data, callback, retries)
                else:
                    raise MaxRetries()
            else:
                raise
        else:
            return callback()
    
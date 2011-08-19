import array
import socket

from kafka.base import BaseKafka, logging, StringIO
socket_log = logging.getLogger('kafka.iostream')

from tornado.iostream import IOStream

__all__ = [
    'Kafka',
]

class KafkaTornado(BaseKafka):
    def __init__(self, *args, **kwargs):
        if 'io_loop' in kwargs:
            self._io_loop = kwargs['io_loop']
            del kwargs['io_loop']
        else:
            self._io_loop = None
        BaseKafka.__init__(self, *args, **kwargs)
        
        self._stream = None

    # Socket management methods

    def _connect(self):
        """ Connect to the Kafka server. """

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        
        try:
            sock.connect((self.host, self.port))
        except Exception, e:
            raise ConnectionFailure("Could not connect to kafka at {0}:{1}".format(self.host, self.port))
        else:
            self._stream = IOStream(sock, io_loop=self._io_loop)

    def _disconnect(self):
        """ Disconnect from the remote server & close the socket. """
        try:
            self._stream.close()
        except IOError:
            pass
        finally:
            self._stream = None

    def _read(self, length, callback=None):
        """ Send a read request to the remote Kafka server. """

        if callback is None:
            callback = lambda v: v
        
        if not self._stream:
            self._connect()
        
        return self._stream.read_bytes(length, callback)

    def _write(self, data, callback=None, retries=BaseKafka.MAX_RETRY):
        """ Write `data` to the remote Kafka server. """

        if callback is None:
            callback = lambda: None

        if not self._stream:
            self._connect()
        
        return self._stream.write(data, callback)

import logging
import time
import unittest
from kafka import Kafka, LATEST_OFFSET, EARLIEST_OFFSET
from kafka.nonblocking import KafkaTornado

try:
    from tornado.testing import AsyncTestCase, LogTrapTestCase
    has_tornado = True
except ImportError:
    has_tornado = False

class TestKafka(unittest.TestCase):
    def test_kafka(self):
        kafka = Kafka()
        topic = '{0}-test-kafka'.format(time.time())
        start_offset = 0
        
        input_messages = ['message0', 'message1', 'message2']
        
        kafka.produce(topic, input_messages)
        fetch_results = kafka.fetch(topic, start_offset)
        
        output_messages = []
        offsets = []
        for offset, output_message in fetch_results:
            output_messages.append(output_message)
            offsets.append(offset)
        
        self.assertEquals(input_messages, output_messages)
        
        actual_latest_offsets = kafka.offsets(topic, LATEST_OFFSET, 
            max_offsets=1)
            
        self.assertEquals(len(actual_latest_offsets), 1)
        self.assertEquals(offsets[-1], actual_latest_offsets[0])
        
        actual_earliest_offsets = kafka.offsets(topic, EARLIEST_OFFSET, 
            max_offsets=1)

        self.assertEquals(len(actual_earliest_offsets), 1)
        self.assertEquals(0, actual_earliest_offsets[0])

if has_tornado:
    class TestKafkaTornado(AsyncTestCase, LogTrapTestCase):
        def test_kafka_tornado(self):
            kafka = KafkaTornado(io_loop=self.io_loop)
            topic = '{0}-test-kafka-tornado'.format(time.time())
            start_offset = 0

            input_messages = ['message0', 'message1', 'message2']

            kafka.produce(topic, input_messages, callback=self.stop)
            self.wait()
            
            kafka.fetch(topic, start_offset, 
                callback=self.stop)
            fetch_results = self.wait()
            
            output_messages = []
            offsets = []
            for offset, output_message in fetch_results:
                output_messages.append(output_message)
                offsets.append(offset)

            self.assertEquals(input_messages, output_messages)

            kafka.offsets(topic, LATEST_OFFSET, 
                max_offsets=1, callback=self.stop)
            actual_latest_offsets = self.wait()

            self.assertEquals(len(actual_latest_offsets), 1)
            self.assertEquals(offsets[-1], actual_latest_offsets[0])

            kafka.offsets(topic, EARLIEST_OFFSET, 
                max_offsets=1, callback=self.stop)
            actual_earliest_offsets = self.wait()
            
            self.assertEquals(len(actual_earliest_offsets), 1)
            self.assertEquals(0, actual_earliest_offsets[0])            


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s,%(msecs)03d %(levelname)-5.5s [%(name)s] %(filename)s:%(lineno)s %(message)s',
        level=logging.DEBUG
    )
    unittest.main()

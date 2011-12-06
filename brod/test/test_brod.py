import logging
import time
import unittest
from brod import (
    Kafka, 
    LATEST_OFFSET, EARLIEST_OFFSET, Lengths, 
    ConnectionFailure,
    OffsetOutOfRange,
    InvalidOffset,
)

from brod.nonblocking import KafkaTornado

try:
    from tornado.testing import AsyncTestCase, LogTrapTestCase
    has_tornado = True
except ImportError:
    has_tornado = False

def get_unique_topic(name):
    return '{0}-{1}'.format(time.time(), name)

class TestKafkaBlocking(unittest.TestCase):
    def test_kafka(self):
        kafka = Kafka()
        topic = get_unique_topic('test-kafka')
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
        expected_latest_offset = offsets[-1] + Lengths.MESSAGE_HEADER \
            + len(output_messages[-1])
        self.assertEquals(expected_latest_offset, actual_latest_offsets[0])
        
        actual_earliest_offsets = kafka.offsets(topic, EARLIEST_OFFSET, 
            max_offsets=1)

        self.assertEquals(len(actual_earliest_offsets), 1)
        self.assertEquals(0, actual_earliest_offsets[0])

    def test_cant_connect(self):
        kafka = Kafka(host=str(time.time()))
        topic = get_unique_topic('test-cant-connect')
    
        self.assertRaises(ConnectionFailure, kafka.produce, topic, 
            'wont appear')

    

if has_tornado:
    class TestKafkaTornado(AsyncTestCase, LogTrapTestCase):
        def test_kafka_tornado(self):
            kafka = KafkaTornado(io_loop=self.io_loop)
            topic = get_unique_topic('test-kafka-tornado')
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
            expected_latest_offset = offsets[-1] + Lengths.MESSAGE_HEADER \
                + len(output_messages[-1])
            self.assertEquals(expected_latest_offset, 
                actual_latest_offsets[0])

            kafka.offsets(topic, EARLIEST_OFFSET, 
                max_offsets=1, callback=self.stop)
            actual_earliest_offsets = self.wait()
            
            self.assertEquals(len(actual_earliest_offsets), 1)
            self.assertEquals(0, actual_earliest_offsets[0])            

        def test_cant_connect(self):
            kafka = KafkaTornado(host=str(time.time()), io_loop=self.io_loop)
            topic = get_unique_topic('test-cant-connect')

            self.assertRaises(ConnectionFailure, kafka.produce, topic, 
                'wont appear')


class TestTopic(unittest.TestCase):
    # Contents of self.dogs_queue after setUp:
    #   [(0, 'Rusty'), (14, 'Patty'), (28, 'Jack'), (41, 'Clyde')]
    
    def setUp(self):
        self.k = Kafka()
        self.topic_name = get_unique_topic('test-kafka-topic')
        input_messages = ['Rusty', 'Patty', 'Jack', 'Clyde']
        self.k.produce(self.topic_name, input_messages)
        
        # If you don't do this sleep, then you can get into a condition where
        # a fetch immediately after a produce will cause a state where the 
        # produce is duplicated (it really gets that way in Kafka).
        time.sleep(1)
        self.dogs_queue = self.k.topic(self.topic_name)
        
        # print list(self.k.fetch(self.topic_name, 0))
        # print self.topic_name
        
    
    def test_offset_queries(self):
        self.assertEqual(self.dogs_queue.earliest_offset(), 0)
        self.assertEqual(self.dogs_queue.latest_offset(), 55)
        self.assertRaises(OffsetOutOfRange, self.dogs_queue.poll(100).next)
        self.assertRaises(InvalidOffset, self.dogs_queue.poll(22).next)

    def test_end_offset_iteration(self):
        dogs = self.dogs_queue.poll(0, end_offset=28, poll_interval=None)
        status, messages = dogs.next()
        self.assertEqual(status.start_offset, 0)
        self.assertEqual(status.next_offset, 41)
        self.assertEqual(status.last_offset_read, 28)
        self.assertEqual(status.messages_read, 3)
        self.assertEqual(status.bytes_read, 14)
        self.assertEqual(status.num_fetches, 1)
        self.assertEqual(messages, ['Rusty', 'Patty', 'Jack'])
        self.assertRaises(StopIteration, dogs.next)
    
        

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s,%(msecs)03d %(levelname)-5.5s [%(name)s] %(filename)s:%(lineno)s %(message)s',
        level=logging.DEBUG
    )
    unittest.main()

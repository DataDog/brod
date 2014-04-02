# brod

__This library is not maintained anymore and only supports Kafka 0.6. Please use [mumrah/kafka-python](https://github.com/mumrah/kafka-python) if you want Kafka 0.8 support.__

brod lets you produce messages to the Kafka distributed publish/subscribe 
messaging service. It started as a fork of pykafka 
(https://github.com/dsully/pykafka), but became a total rewrite as we needed to
add many features.

It's named after Max Brod, Franz Kafka's friend and supporter.

## Requirements

You need to have access to your Kafka instance and be able to connect through
TCP. You can obtain a copy and instructions on how to setup kafka at
http://incubator.apache.org/kafka/

## Installation

`easy_install brod`

**Note:** the `zc.zk` package has a dependency on Python Zoo Keeper bindings which are not included during it's installation. They can be installed with `easy_install zc-zookeeper-static` see the `zc.zk` documentation for more information http://pypi.python.org/pypi/zc.zk/0.5.

## Usage

### Sending a simple message

    import brod
    kafka = brod.Kafka(host='localhost')
    kafka.produce("test-topic", "Hello World")

### Sending a sequence of messages

    import brod
    kafka = brod.Kafka(host='localhost')
    kafka.produce("test-topic", ["Hello", "World"])

### Consuming messages one by one

    import brod
    kafka = brod.Kafka(host='localhost')
    for offset, message in brod.fetch("test-topic", offset=0):
        print message

### Using a ZooKeeper-based consumer

    from brod.zk import ZKConsumer

    consumer = ZKConsumer('zk_host:2181', 'my_consumer_group', 'my_topic', autocommit=True)

    # Polls forever
    for msg_set in consumer.poll(poll_interval=1):
        for offset, msg in msg_set:
            print offset, msg_set.broker_partition, msg

### Nonblocking Tornado client support

```python
import time
import tornado.ioloop
import tornado.web

from brod import LATEST_OFFSET
from brod.nonblocking import KafkaTornado

class MainHandler(tornado.web.RequestHandler):
def initialize(self, kafka, topic):
self.kafka = kafka
self.topic = topic

def post(self):
data = self.get_argument('data')
self.kafka.produce(self.topic, data)

@tornado.web.asynchronous
def get(self):
brod.offsets(self.topic, LATEST_OFFSET, max_offsets=2, 
callback=self._on_offset)

def _on_offset(self, offsets):
offset = offsets[-1] # Get the second to latest offset
brod.fetch(self.topic, offset, callback=self._on_fetch)

def _on_fetch(self, messages):
for offset, message in messages:
self.write("{0}: {1}".format(offset, message))
self.finish()


kafka = KafkaTornado()

application = tornado.web.Application([
(r"/", MainHandler, {
'kafka': kafka,
'topic': 'hello-world'
}),
])

if __name__ == "__main__":
parse_command_line()
application.listen(8888)
tornado.ioloop.IOLoop.instance().start()
```
    

Contact:

Please use the GitHub issues: https://github.com/datadog/brod/issues

* Forked from https://github.com/dsully/pykafka which was inspired by Alejandro Crosa's kafka-rb: https://github.com/acrosa/kafka-rb

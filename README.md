# pykafka

pykafka allows you to produce messages to the Kafka distributed publish/subscribe messaging service.

## Requirements

You need to have access to your Kafka instance and be able to connect through
TCP. You can obtain a copy and instructions on how to setup kafka at
https://github.com/kafka-dev/kafka

## Installation
pip install pykafka

## Usage

### Sending a simple message

    import kafka

    producer = kafka.producer.Producer('test')
    message  = kafka.message.Message("Foo!")
    producer.send(message)

### Sending a sequence of messages

    import kafka

    producer = kafka.producer.Producer('test')
    message1 = kafka.message.Message("Foo!")
    message2 = kafka.message.Message("Bar!")
    producer.send([message1, message2])

### Batching a bunch of messages using a context manager.

    import kafka
    producer = kafka.producer.Producer('test')

    with producer.batch() as messages:
      print "Batching a send of multiple messages.."
      messages.append(kafka.message.Message("first message to send")
      messages.append(kafka.message.Message("second message to send")

* they will be sent all at once, after the context manager execution.

### Consuming messages one by one

    import kafka
    consumer = kafka.consumer.Consumer('test')
    messages = consumer.consume()

### Consuming messages using a generator loop

    import kafka

    consumer = kafka.consumer.Consumer('test')

    for message in consumer.loop():
      print message

Contact:

Please use the GitHub issues: https://github.com/dsully/pykafka/issues

* Inspiried from Alejandro Crosa's kafka-rb: https://github.com/acrosa/kafka-rb

# pykafka

pykafka allows you to produce messages to the Kafka distributed publish/subscribe messaging service.

## Requirements

You need to have access to your Kafka instance and be able to connect through
TCP. You can obtain a copy and instructions on how to setup kafka at
https://github.com/kafka-dev/kafka

## Installation
easy_install -f 'https://github.com/DataDog/pykafka/tarball/2.1.0#egg=pykafka-2.1.0' pykafka

## Usage

### Sending a simple message

    import kafka
    kafka = kafka.Kafka(host='localhost')
    kafka.produce("test-topic", "Hello World")

### Sending a sequence of messages

    import kafka
    kafka = kafka.Kafka(host='localhost')
    kafka.produce("test-topic", ["Hello", "World"])

### Consuming messages one by one

    import kafka
    kafka = kafka.Kafka(host='localhost')
    for offset, message in kafka.fetch("test-topic", offset=0):
        print message


Contact:

Please use the GitHub issues: https://github.com/datadog/pykafka/issues

* Forked from https://github.com/dsully/pykafka which was inspiried by Alejandro Crosa's kafka-rb: https://github.com/acrosa/kafka-rb

"""
This runs tests for brod's ZooKeeper Producer and Consumer. It actually starts
an instance of ZooKeeper and multiple instances of Kafka. Since it's meant to 
run during development, it runs these servers on different ports and different
locations from the application defaults, to avoid conflicts.

Current assumptions this script makes:

1. ZooKeeper will be run on port 2182 (2181 is standard)
2. Kafka will run on ports 9101 and up, depending on the number of instances.
   (9092 is the usual port.)
3. You have Kafka installed and Kafka's /bin directory is in your PATH. In 
   particular, the tests require kafka-run-class.sh
4. Kafka will use the JMX ports from 10000 and up, depending on the number of
   instances. This is not something we can set in the config file -- we have
   to pass it in the form of an environment var JMX_PORT.
5. You have a /tmp directory
6. You don't run any of the kafka shutdown scripts in kafka/bin while these
   tests are running. (kafka-server-stop.sh for instance just greps for all
   processes with kafka.Kafka in it and kills them -- so it'll bork our tests)

What you need to run the tests:

Note that the test order matters. Bringing ZooKeeper and the Kafka brokers up
takes a few seconds each time, so we try to avoid it when possible. The tests
are run in the order that they appear in the module, and at any time, we can
change the Kafka server topology by using nosetest's @with_setup decorator and
passing it a ServerTopology object. By convention, the test functions are named
after the topology that they're using. For example:

topology_3x5 = ServerTopology("3x5", 3, 5) # 3 brokers, 5 partitions each

@with_setup(setup_servers(topology_3x5)) 
def test_3x5_something():
    ...

def test_3x5_something_else():
    ...

topology_1x1 = ServerTopology("1x1", 1, 1) # 1 broker, 1 partition

@with_setup(setup_servers(topology_1x1)) 
def test_1x1_something():
    ...

def test_1x1_something_else():
    ...

If you have a test that you want to run with multiple topologies:

1. Make the real test function a parameterized one that doesn't start or end
   with "test" (so that nose doesn't automatically pick it up).
2. Make a test in the appropriate server topology grouping that just calls the
   real test with the appropriate arguments.

Finally: Try to use different consumer group names if at all possible, so that
the tests with the same topology setup won't interfere with each other. I've 
adopted the convention of using the function names in the group names.

TODO/FIXME:
1. We could save time at the expense of memory if we're willing to spin up all
   the servers for our various configurations at once (or at least a decent 
   group of them). But we'd have to refactor how we do the tracking, since 
   right now all the run start/stop stuff is in static vars of RunConfig

2. There's a whole lot of noise in the logs that comes out when you shut down
   a ZooKeeper instance and the underlying ZooKeeper library freaks out about
   the lost connections for the various notification subscriptions it's made.
   I tried forcing garbage collection and increasing the wait time after the
   servers are torn down, but something in the ZooKeeper library isn't getting
   deleted properly when things go out of scope. The upshot is that devs might
   get lots of superflous ZooKeeper errors in their logs.

3. There's currently no testing of what happens to us in the case of ZooKeeper 
   failures. Though Kafka itself relies on ZooKeeper in the default 
   configuration, so I don't know if there's much we can do aside from fail
   with a clear error message.
"""
import logging
import os
import os.path
import signal
import subprocess
import time
from collections import namedtuple
from datetime import datetime
from functools import partial
from itertools import chain
from subprocess import Popen
from unittest import TestCase

from nose.tools import *

from zc.zk import ZooKeeper

from brod import Kafka, ConnectionFailure
from brod.zk import ZKConsumer, ZKProducer

class ServerTopology(namedtuple('ServerTopology',
                                'name num_brokers partitions_per_broker')):
    @property
    def total_partitions(self):
        return self.num_brokers * self.partitions_per_broker

ZKConfig = namedtuple('ZKConfig', 'config_file data_dir client_port')
KafkaConfig = namedtuple('KafkaConfig', 
                         'config_file broker_id port log_dir ' + \
                         'num_partitions zk_server jmx_port')

KAFKA_BASE_PORT = 9101
JMX_BASE_PORT = 10000
ZK_PORT = 2182
ZK_CONNECT_STR = "localhost:{0}".format(ZK_PORT)

# Messages are not available to clients until they have been flushed.
# By default is is 1000ms, see log.default.flush.interval.ms in 
# server.properties
MESSAGE_DELAY_SECS = (1000 * 2) / 1000

log = logging.getLogger("brod")

class KafkaServer(object):

    def __init__(self, kafka_config):
        self.kafka_config = kafka_config
        self.process = None

    def start(self):
        if self.process:
            return

        env = os.environ.copy()
        env["JMX_PORT"] = str(self.kafka_config.jmx_port)
        log.info("SETUP: Starting Kafka with config {0}".format(self.kafka_config))
        run_log = "kafka_{0}.log".format(self.kafka_config.broker_id)
        run_errs = "kafka_error_{0}.log".format(self.kafka_config.broker_id)
        self.process = Popen(["kafka-run-class.sh",
                              "kafka.Kafka", 
                              self.kafka_config.config_file],
                             stdout=open("{0}/{1}".format(RunConfig.run_dir, run_log), "wb"),
                             stderr=open("{0}/{1}".format(RunConfig.run_dir, run_errs), "wb"),
                             shell=False,
                             preexec_fn=os.setsid,
                             env=env)
    
    def stop(self):
        if self.process:
            log.info("TEARDOWN: Terminating Kafka process {0}".format(self.process))
            os.killpg(self.process.pid, signal.SIGTERM)
            self.process = None

    @classmethod
    def setup(cls, num_instances, num_partitions):
        config_dir, data_dir = create_run_dirs("kafka/config", "kafka/data")

        # Write this session's config file...
        configs = []
        for i in range(num_instances):
            config_file = os.path.join(config_dir, 
                                       "kafka.{0}.properties".format(i))
            log_dir = os.path.join(data_dir, str(i))
            os.makedirs(log_dir)
            kafka_config = KafkaConfig(config_file=config_file,
                                       broker_id=i,
                                       port=KAFKA_BASE_PORT + i,
                                       log_dir=log_dir,
                                       num_partitions=num_partitions,
                                       zk_server="localhost:{0}".format(ZK_PORT),
                                       jmx_port=JMX_BASE_PORT + i)
            configs.append(kafka_config)
            write_config("kafka.properties", config_file, kafka_config)

        return [KafkaServer(c) for c in configs]
        

class RunConfig(object):
    """This container class just has a bunch of class level vars that are 
    manipulated by each setup_servers()/teardown() call. At any given point, it
    has the configuration state used by the current run of ZooKeeper + Kafka.

    Don't directly reset these values yourself. If you need a new configuration
    for a set of tests, give your test the decorator:
        topology_3x5 = ServerTopology("3x5", 3, 5)
        @with_setup(setup_servers(topology_3x5))
        def test_something():
            # do stuff here
    """
    kafka_servers = None
    run_dir = None
    zk_config = None
    zk_process = None

    @classmethod
    def clear(cls):
        cls.kafka_servers = cls.run_dir = cls.zk_config = cls.zk_process = None

    @classmethod
    def is_running(cls):
        return any([cls.kafka_servers, cls.run_dir, cls.zk_config, cls.zk_process])

def setup_servers(topology):
    def run_setup():
        # For those tests that ask for new server instances -- kill the old one.
        if RunConfig.is_running():
            teardown()

        timestamp = datetime.now().strftime('%Y-%m-%d-%H_%M_%s_%f')
        RunConfig.run_dir = os.path.join("/tmp", "brod_zk_test", timestamp)
        os.makedirs(RunConfig.run_dir)
        log.info("SETUP: Running with toplogy {0}".format(topology))
        log.info(("SETUP: {0.num_brokers} brokers, {0.partitions_per_broker} " +
                  "partitions per broker.").format(topology))
        log.info("SETUP: ZooKeeper and Kafka data in {0}".format(RunConfig.run_dir))

        # Set up configuration and data directories for ZK and Kafka
        RunConfig.zk_config = setup_zookeeper()

        # Start ZooKeeper...
        log.info("SETUP: Starting ZooKeeper with config {0}"
                 .format(RunConfig.zk_config))
        RunConfig.zk_process = Popen(["kafka-run-class.sh",
                                       "org.apache.zookeeper.server.quorum.QuorumPeerMain",
                                       RunConfig.zk_config.config_file],
                                      stdout=open(RunConfig.run_dir + "/zookeeper.log", "wb"),
                                      stderr=open(RunConfig.run_dir + "/zookeeper_error.log", "wb"),
                                      shell=False,
                                      preexec_fn=os.setsid)
        # Give ZK a little time to finish starting up before we start spawning
        # Kafka instances to connect to it.
        time.sleep(3)

        # Start Kafka. We use kafka-run-class.sh instead of 
        # kafka-server-start.sh because the latter sets the JMX_PORT to 9999
        # and we want to set it differently for each Kafka instance
        RunConfig.kafka_servers = KafkaServer.setup(topology.num_brokers, 
                                                    topology.partitions_per_broker)
        for kafka_server in RunConfig.kafka_servers:
            kafka_server.start()
        
        # Now give the Kafka instances a little time to spin up...
        time.sleep(3)
    
    return run_setup

def setup_zookeeper():
    # Create all the directories we need...
    config_dir, data_dir = create_run_dirs("zookeeper/config", "zookeeper/data")
    # Write this session's config file...
    config_file = os.path.join(config_dir, "zookeeper.properties")
    zk_config = ZKConfig(config_file, data_dir, ZK_PORT)
    write_config("zookeeper.properties", config_file, zk_config)

    return zk_config

def teardown():
    # Have to kill Kafka before ZooKeeper, or Kafka will get very distraught
    # You can't kill the processes with Popen.terminate() because what we
    # call is just a shell script that spawns off a Java process. But since
    # we did that bit with preexec_fn=os.setsid when we created them, we can
    # kill the entire process group with os.killpg
    if not RunConfig.is_running():
        return

    for kafka_server in RunConfig.kafka_servers:
        kafka_server.stop()

    log.info("TEARDOWN: Terminating ZooKeeper process {0}"
             .format(RunConfig.zk_process))
    os.killpg(RunConfig.zk_process.pid, signal.SIGTERM)
    time.sleep(1)
    RunConfig.clear()

def terminate_process(process):
    os.killpg(process.pid, signal.SIGTERM)

def write_config(template_name, finished_location, format_obj):
    with open(template(template_name)) as template_file:
        template_text = template_file.read()
        config_text = template_text.format(format_obj)
        with open(finished_location, "wb") as finished_file:
            finished_file.write(config_text)

def create_run_dirs(*dirs):
    paths = [os.path.join(RunConfig.run_dir, d) for d in dirs]
    for path in paths:
        os.makedirs(path)
    return paths

def template(config):
    """Return the template configuration file for a given config file."""
    script_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(script_dir, "server_config", config)

####################### Util methods to use during tests #######################

def print_zk_snapshot():
    # Dump all the ZooKeeper state at this point
    zk = ZooKeeper(ZK_CONNECT_STR)
    print zk.export_tree(ephemeral=True)

def log_break(method_name):
    """Try to make it a little easier to see where we switch to different 
    sections in the log"""
    log.info("")
    log.info("*************************** {0} ***************************"
             .format(method_name))
    log.info("")

def send_to_all_partitions(topic, messages):
    for kafka_server in RunConfig.kafka_servers:
        k = Kafka("localhost", kafka_server.kafka_config.port)
        for partition in range(topology_3x5.partitions_per_broker):
            k.produce(topic, messages, partition)

################################ TESTS BEGIN ###################################

topology_3x5 = ServerTopology("3x5", 3, 5) # 3 brokers, 5 partitions each

@with_setup(setup_servers(topology_3x5)) 
def test_3x5_consumer_rebalancing():
    """Consumer rebalancing, with auto rebalancing."""
    log_break("test_3x5_consumer_rebalancing")
    for kafka_server in RunConfig.kafka_servers:
        k = Kafka("localhost", kafka_server.kafka_config.port)
        for topic in ["t1", "t2", "t3"]:
            k.produce(topic, ["bootstrap"], 0)
    time.sleep(MESSAGE_DELAY_SECS)

    producer = ZKProducer(ZK_CONNECT_STR, "t1")
    assert_equals(len(producer.broker_partitions), topology_3x5.total_partitions,
                  "We should be sending to all broker_partitions.")
           
    c1 = ZKConsumer(ZK_CONNECT_STR, "group_3x5", "t1")
    assert_equals(len(c1.broker_partitions), topology_3x5.total_partitions,
                  "Only one consumer, it should have all partitions.")
    c2 = ZKConsumer(ZK_CONNECT_STR, "group_3x5", "t1")
    assert_equals(len(c2.broker_partitions), (topology_3x5.total_partitions) / 2)

    time.sleep(MESSAGE_DELAY_SECS)
    assert_equals(len(set(c1.broker_partitions + c2.broker_partitions)),
                  topology_3x5.total_partitions,
                  "We should have all broker partitions covered.")

    c3 = ZKConsumer(ZK_CONNECT_STR, "group_3x5", "t1")
    assert_equals(len(c3.broker_partitions), (topology_3x5.total_partitions) / 3)

    time.sleep(MESSAGE_DELAY_SECS)
    assert_equals(sum(len(c.broker_partitions) for c in [c1, c2, c3]),
                  topology_3x5.total_partitions,
                  "All BrokerPartitions should be accounted for.")
    assert_equals(len(set(c1.broker_partitions + c2.broker_partitions + 
                          c3.broker_partitions)),
                  topology_3x5.total_partitions,
                  "There should be no overlaps")

def test_3x5_consumers():
    """Multi-broker/partition fetches"""
    log_break("test_3x5_consumers")
    c1 = ZKConsumer(ZK_CONNECT_STR, "group_3x5_consumers", "topic_3x5_consumers")
    
    result = c1.fetch()
    assert_equals(len(result), 0, "This shouldn't error, but it should be empty")

    send_to_all_partitions("topic_3x5_consumers", ["hello"])
    time.sleep(MESSAGE_DELAY_SECS)

    # This should grab "hello" from every partition and every topic
    # c1.rebalance()
    result = c1.fetch()

    assert_equals(len(set(result.broker_partitions)), topology_3x5.total_partitions)
    for msg_set in result:
        assert_equals(msg_set.messages, ["hello"])

def test_3x5_zookeeper_invalid_offset():
    """Test recovery from bad ZK offset value

    If ZooKeeper stored an invalid start offset, we should print an ERROR
    and start from the latest."""
    log_break("test_3x5_zookeeper_invalid_offset")

    c1 = ZKConsumer(ZK_CONNECT_STR, 
                    "group_3x5_zookeeper_invalid_offset", 
                    "topic_3x5_zookeeper_invalid_offset",
                    autocommit=True)
    
    send_to_all_partitions("topic_3x5_zookeeper_invalid_offset", ["hello"])
    time.sleep(MESSAGE_DELAY_SECS)

    # The following fetch will also save the ZK offset (autocommit=True)
    result = c1.fetch()

    # Now let's reach into ZooKeeper and manually set the offset to something 
    # out of range.
    z1 = c1._zk_util
    bps_to_fake_offsets = dict((bp, 1000) for bp in c1.broker_partitions)
    z1.save_offsets_for(c1.consumer_group, bps_to_fake_offsets)
    c1.close()
    time.sleep(MESSAGE_DELAY_SECS)

    # Now delete c1, and create c2, which will take over all of it's partitions
    c2 = ZKConsumer(ZK_CONNECT_STR, 
                    "group_3x5_zookeeper_invalid_offset", 
                    "topic_3x5_zookeeper_invalid_offset",
                    autocommit=True)
    # This should detect that the values in ZK are bad, and put us at the real
    # end offset.
    c2.fetch()
               
    send_to_all_partitions("topic_3x5_zookeeper_invalid_offset", ["world"])
    time.sleep(MESSAGE_DELAY_SECS)

    result = c2.fetch()
    assert result
    for msg_set in result:
        assert_equals(msg_set.messages, ["world"])

# Make sure that even if the test fails, the instance we brought down starts
# back up.
@with_setup(teardown=lambda: RunConfig.kafka_servers[0].start())
def test_3x5_reconnects():
    """Test that we keep trying to read, even if our brokers go down.

    We're going to:

    1. Send messages to all partitions in a topic, across all brokers
    2. Do a fetch (this will cause the Consumer to rebalance itself and find
       everything).
    3. Set the Consumer to disable rebalancing.
    4. Shut down one of the brokers
    5. Assert that nothing blows up
    6. Restart the broker and assert that it continues to run.

    Note that the partition split is always based on what's in ZooKeeper. So 
    even if the broker is dead or unreachable, we still keep its partitions and 
    try to contact it. Maybe there's a firewall issue preventing our server from
    hitting it. We don't want to risk messing up other consumers by grabbing
    partitions that might belong to them.
    """
    send_to_all_partitions("topic_3x5_reconnects", ["Rusty"])
    time.sleep(MESSAGE_DELAY_SECS)

    c1 = ZKConsumer(ZK_CONNECT_STR, "group_3x5_reconnects", "topic_3x5_reconnects")
    result = c1.fetch()
    assert_equal(topology_3x5.total_partitions, len(result))
    for msg_set in result:
        assert_equal(msg_set.messages, ["Rusty"])

    # Now send another round of messages to our broker partitions
    send_to_all_partitions("topic_3x5_reconnects", ["Jack"])
    time.sleep(MESSAGE_DELAY_SECS)

    # Disable rebalancing to force the consumer to read from the broker we're 
    # going to kill, and then kill it.
    c1.disable_rebalance()
    fail_server = RunConfig.kafka_servers[0]
    fail_server.stop()
    time.sleep(MESSAGE_DELAY_SECS)

    # A straight fetch will give us a connection failure because it couldn't
    # reach the first broker. It won't increment any of the other partitions --
    # the whole thing should fail without any side effect.
    assert_raises(ConnectionFailure, c1.fetch)

    # But a fetch told to ignore failures will return the results from the 
    # brokers that are still up
    result = c1.fetch(ignore_failures=True)
    assert_equal(topology_3x5.total_partitions - topology_3x5.partitions_per_broker,
                 len(result))
    for msg_set in result:
        assert_equal(msg_set.messages, ["Jack"])

    # Now we restart the failed Kafka broker, and do another fetch...
    fail_server.start()
    time.sleep(MESSAGE_DELAY_SECS)

    result = c1.fetch()
    # This should have MessageSets from all brokers (they're all reachable)
    assert_equal(topology_3x5.total_partitions, len(result))
    # But the only MessageSets that have messages in them should be from our
    # fail_server (the others were already read in a previous fetch, so will be
    # empty on this fetch).
    assert_equal(topology_3x5.total_partitions - topology_3x5.partitions_per_broker,
                 len([msg_set for msg_set in result if not msg_set]))
    # The messages from our resurrected fail_server will be "Jack"s
    assert_equal(topology_3x5.partitions_per_broker,
                 len([msg_set for msg_set in result
                      if msg_set.messages == ["Jack"]]))

def test_3x5_producer_bootstrap():
    """Test that ZKProducers properly bootstrap themselves when creating new 
    topics. The problem is like this:

    When a Producer is created, it knows what brokers are available.

    """
    topic = "topic_3x5_producer_bootstrap"
    p1 = ZKProducer(ZK_CONNECT_STR, topic)

    # This is a new topic, and the Producer can't know how many partitions are.
    # Therefore, it should assume just the 0th partition for each broker is safe
    assert_equal(len(p1.broker_partitions), topology_3x5.num_brokers)
    broker_partition = p1.send(["hi"])

    # It could have sent it to any of the brokers, but it should only have sent
    # to the 0th partition of that broker.
    assert_equal(broker_partition.partition, 0)

    time.sleep(2)
    # But now, enough time should have passed that the broker we sent it to has
    # published to ZooKeeper how many partitions it really has for this topic.
    # So the number of broker partitions detected would be:
    #    (1 broker * number of partitions) + (2 brokers * 1 partition each)
    assert_equal(len(p1.broker_partitions),
                 topology_3x5.partitions_per_broker + (topology_3x5.num_brokers - 1))

    














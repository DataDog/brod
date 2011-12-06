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
5. You have env installed (i.e. you're running on a UNIX machine)
6. You have a /tmp directory

Note that the test order matters. Bringing ZooKeeper and the Kafka brokers up
takes around 6 seconds or so, so we should try to avoid it. In general, the 
tests should be self contained and try to return things to the state they were
if possible.

We'll probably have to allow the world to be reset eventually.
"""
import logging
import os
import os.path
import signal
import subprocess
import time
from collections import namedtuple
from datetime import datetime
from subprocess import Popen
from unittest import TestCase

from zc.zk import ZooKeeper

from brod.zk import ZKProducer

ZKConfig = namedtuple('ZKConfig', 'config_file data_dir client_port')
KafkaConfig = namedtuple('KafkaConfig', 
                         'config_file broker_id port log_dir ' + \
                         'num_partitions zk_server jmx_port')
KAFKA_BASE_PORT = 9101
JMX_BASE_PORT = 10000
ZK_PORT = 2182
ZK_CONNECT_STR = "localhost:{0}".format(ZK_PORT)

NUM_BROKERS = 3 # How many Kafka brokers we're going to spin up
NUM_PARTITIONS = 5 # How many partitions per topic per broker

log = logging.getLogger("brod.test_zk")

class TestZK(TestCase):

    def test_001_brokers_all_up(self):
        producer = ZKProducer(ZK_CONNECT_STR, "topic_001")
        self.assertEquals(len(producer.broker_partitions), 
                          NUM_BROKERS * NUM_PARTITIONS)

    def setUp(self):
        timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%s')
        self.run_dir = os.path.join("/tmp", "brod_zk_test", timestamp)
        os.makedirs(self.run_dir)
        log.info("ZooKeeper and Kafka data in {0}".format(self.run_dir))

        # Set up configuration and data directories for ZK and Kafka
        self.zk_config = self.setup_zookeeper()
        self.kafka_configs = self.setup_kafka(NUM_BROKERS, NUM_PARTITIONS)

        # Start ZooKeeper...
        log.info("Starting ZooKeeper with config {0}".format(self.zk_config))
        self.zk_process = Popen(["kafka-run-class.sh",
                                 "org.apache.zookeeper.server.quorum.QuorumPeerMain",
                                 self.zk_config.config_file],
                                stdout=open(self.run_dir + "/zookeeper.log", "wb"),
                                stderr=open(self.run_dir + "/zookeeper_error.log", "wb"),
                                shell=False,
                                preexec_fn=os.setsid
                          )
        # Give ZK a little time to finish starting up before we start spawning
        # Kafka instances to connect to it.
        time.sleep(3)

        # Start Kafka. We use kafka-run-class.sh instead of 
        # kafka-server-start.sh because the latter sets the JMX_PORT to 9999
        # and we want to set it differently for each Kafka instance
        self.kafka_processes = []
        for kafka_config in self.kafka_configs:
            env = os.environ.copy()
            env["JMX_PORT"] = str(kafka_config.jmx_port)
            log.info("Starting Kafka with config {0}".format(kafka_config))
            run_log = "kafka_{0}.log".format(kafka_config.broker_id)
            run_errs = "kafka_error_{0}.log".format(kafka_config.broker_id)
            process = Popen(["kafka-run-class.sh",
                             "kafka.Kafka", 
                             kafka_config.config_file],
                            stdout=open("{0}/{1}".format(self.run_dir, run_log), "wb"),
                            stderr=open("{0}/{1}".format(self.run_dir, run_errs), "wb"),
                            shell=False,
                            preexec_fn=os.setsid,
                            env=env,
                      )
            self.kafka_processes.append(process)
        
        # Now give the Kafka instances a little time to spin up...
        time.sleep(3)

    def setup_zookeeper(self):
        # Create all the directories we need...
        config_dir, data_dir = self._create_run_dirs("zookeeper/config",
                                                     "zookeeper/data")

        # Write this session's config file...
        config_file = os.path.join(config_dir, "zookeeper.properties")
        zk_config = ZKConfig(config_file, data_dir, ZK_PORT)
        self._write_config("zookeeper.properties", config_file, zk_config)

        return zk_config

    def setup_kafka(self, num_instances, num_partitions):
        config_dir, data_dir = self._create_run_dirs("kafka/config", "kafka/data")

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
            self._write_config("kafka.properties", config_file, kafka_config)

        return configs

    def tearDown(self):
        # Have to kill Kafka before ZooKeeper, or Kafka will get very distraught
        # You can't kill the processes with Popen.terminate() because what we
        # call is just a shell script that spawns off a Java process. But since
        # we did that bit with preexec_fn=os.setsid when we created them, we can
        # kill the entire process group with os.killpg
        for process in self.kafka_processes:
            log.info("Terminating Kafka process {0}".format(process))
            os.killpg(process.pid, signal.SIGTERM)

        log.info("Terminating ZooKeeper process {0}".format(self.zk_process))
        os.killpg(self.zk_process.pid, signal.SIGTERM)

    def _write_config(self, template_name, finished_location, format_obj):
        with open(self._template(template_name)) as template_file:
            template_text = template_file.read()
            config_text = template_text.format(format_obj)
            with open(finished_location, "wb") as finished_file:
                finished_file.write(config_text)

    def _create_run_dirs(self, *dirs):
        paths = [os.path.join(self.run_dir, d) for d in dirs]
        for path in paths:
            os.makedirs(path)
        return paths
    
    def _template(self, config):
        """Return the template configuration file for a given config file."""
        script_dir = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(script_dir, "server_config", config)

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
   particular, the tests require zookeeper-server-start.sh and 
   kafka-server-start.sh
4. Kafka will use the JMX ports from 10000 and up, depending on the number of
   instances. This is not something we can set in the config file -- we have
   to pass it in the form of an environment var JMX_PORT.
5. You have env installed (i.e. you're running on a UNIX machine)
6. You have a /tmp directory
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

ZKConfig = namedtuple('ZKConfig', 'config_file data_dir client_port')
KafkaConfig = namedtuple('KafkaConfig', 
                         'config_file broker_id port log_dir ' + \
                         'num_partitions zk_server jmx_port')
ZK_PORT = 2182
KAFKA_BASE_PORT = 9101
JMX_BASE_PORT = 10000

log = logging.getLogger("brod.test_zk")

class TestZK(TestCase):

    def setUp(self):
        timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%s')
        self.run_dir = os.path.join("/tmp", "brod_zk_test", timestamp)
        os.makedirs(self.run_dir)
        log.info("ZooKeeper and Kafka data in {0}".format(self.run_dir))

        # Set up configuration and data directories for ZK and Kafka
        self.zk_config = self.setup_zookeeper()
        self.kafka_configs = self.setup_kafka(3, 5) # 3 instances, 5 partitions
        print self.kafka_configs

        # Start ZooKeeper...
        log.info("Starting ZooKeeper with config {0}".format(self.zk_config))
        self.zk_process = Popen(["kafka-run-class.sh",
                                 "org.apache.zookeeper.server.quorum.QuorumPeerMain",
                                 self.zk_config.config_file],
                                stdout=open(self.run_dir + "/zookeeper.log", "wb"),
                                stderr=open(self.run_dir + "/zookeeper_error.log", "wb"),
                                shell=False
                          )
        # Give ZK a little time to finish starting up before we start spawning
        # Kafka instances to connect to it.
        time.sleep(5)

        # Start Kafka...
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
                            env=env,
                      )
            self.kafka_processes.append(process)
        
        # Now give the Kafka instances a little time to spin up...
        time.sleep(5)

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
        # You can't kill shell-spawned processes with process.terminate(), so
        # I'm doing this kludge until I run across a better way.
        for process in self.kafka_processes:
            log.info("Terminating Kafka process {0}".format(process))
            process.terminate()
            process.wait()
            # subprocess.call("kill {0}".format(process.pid), shell=True)

        log.info("Terminating ZooKeeper process {0}".format(self.zk_process))
        # os.killpg(self.zk_process.pid, signal.SIGTERM)
        # self.zk_process.wait()
        self.zk_process.terminate()
        self.zk_process.wait()

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

    def test_basics(self):
        print "hello world"
        assert(False)
        # raise Exception("Just want to trigger logs")

        # p = producer.ZKProducer(ZooKeeper(zkaddr=2182), "steve")
        #print p._broker_partitions
        #print p._connections
        #for i in range(2):
        #    print p.send(["hi %s" % i], key=i)
        # assert(p._broker_partitions == None)


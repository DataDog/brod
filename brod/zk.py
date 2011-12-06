import json
import logging
import uuid
from collections import namedtuple, Mapping
from itertools import chain

import zookeeper
from zc.zk import ZooKeeper

from brod.base import KafkaError
from brod.blocking import Kafka

log = logging.getLogger('brod.zk')

class NoAvailablePartitionsError(KafkaError): pass

class BrokerPartition(namedtuple('BrokerPartition', 
                                 'broker_id creator host port topic partition')):
    @classmethod
    def from_zk(cls, broker_id, broker_string, topic, num_parts):
        """Generate a list of BrokerPartition objects based on various values
        taken from ZooKeeper.

        broker_id is this broker's ID to ZooKeeper. It's a simple integer, set
        as the "brokerid" param in Kafka's server config file. You can find a
        list of them by asking for the children of /brokers/ids in ZooKeeper.

        broker_string is found in ZooKeeper at /brokers/ids/{broker_id}
        The format of broker_string is assumed to be "creator:host:port",
        though the creator can have the host embedded in it because of the
        version of UUID that Kafka uses.

        num_parts is the number of partitions for that broker and is located at
        /brokers/topics/{topic}/{broker_id}
        """
        creator, host, port = broker_string.split(":")
        num_parts = int(num_parts)

        return [BrokerPartition(broker_id=int(broker_id), creator=creator,
                                host=host, port=int(port), topic=topic,
                                partition=i) 
                for i in range(num_parts)]

class ZKUtil(object):

    # This is a free-for-all ACL that we should re-evaluate later
    ACL = [{"perms": 0x1f, "scheme": "world", "id": "anyone"}]

    """Abstracts all Kafka-specific ZooKeeper access."""
    def __init__(self, zk_conn_str):
        self._zk = ZooKeeper(zk_conn_str)

    def broker_partitions_for(self, topic):
        """Return a list of BrokerPartitions based on values found in 
        ZooKeeper."""
        # Get the broker_ids first...
        broker_ids = self.broker_ids_for(topic)
        log.debug(u"broker_ids: {0}".format(broker_ids))

        # Then the broker_strings for each broker
        broker_paths = map(paths.broker, broker_ids)
        log.debug(u"broker_paths: {0}".format(broker_paths))

        broker_strings = map(self._properties, broker_paths)
        log.debug(u"broker_strings: {0}".format(broker_strings))

        # Then the num_parts per broker (each could be set differently)
        broker_topic_paths = [paths.broker_for_topic(broker_id, topic) 
                              for broker_id in broker_ids]
        num_parts = map(self._properties, broker_topic_paths)
        log.debug(u"num_parts: {0}".format(num_parts))
        
        # BrokerPartition
        return list(
                   chain.from_iterable(
                       BrokerPartition.from_zk(broker_id, broker_string, topic, n)
                       for broker_id, broker_string, n
                       in zip(broker_ids, broker_strings, num_parts)
                   )
               )

    def broker_ids_for(self, topic):
        topic_path = self._path_for_topic(topic)
        return sorted(int(broker_id) for broker_id in self._zk_children(topic_path))

    def consumer_ids_for(self, topic, consumer_group):
        """For a given consumer group, return a list of all consumer_ids that
        are currently registered in that group."""
        # Gets the ids node below which are all consumer_ids in this group
        cg_path = self._zk_path(self._path_for_consumer_ids(consumer_group))

        # All consumer_ids for this group, but not all of them are necessarily
        # interested in our topic
        consumer_ids_in_group = sorted(self._zk_util.children(cg_path).data)
        consumer_id_paths = [self._path_for_consumer_id(consumer_id)
                             for consumer_id in consumer_ids_in_group]
        consumer_id_data = [self._zk_util.properties(path).data
                            for path in consumer_id_paths]

        return [consumer_id for consumer_id, data
                in zip(consumer_ids_in_group, consumer_id_data)
                if topic in data]

    def register_consumer(self, consumer_group, consumer_id, topic):
        """Creates the following permanent node, if it does not exist already:
            /consumers/{consumer_group}/ids

        The data written at this node is just the consumer_id so that we can 
        later track who created what.

        We then create the following emphemeral node:
            /consumers/{consumer_group}/ids/{consumer_id}
        
        The data written in this node is a dictionary of topic names (in 
        unicode) to the number of threads that this consumer has registered
        for this topic (in our case, always one).
        """
        self._create_path(self._path_for_consumer_ids(consumer_group), consumer_id)

        # Create an emphermal node for this consumer
        zookeeper.create(self._zk.handle, 
                         u"{0}/{1}".format(consumer_ids_path, consumer_id),
                         json.dumps({topic : 1}), # topic : # of threads
                         ZKUtil.ACL,
                         zookeeper.EPHEMERAL)

    def _create_path_if_needed(self, path, data=None):
        """Creates permanent nodes for all elements in the path if they don't
        already exist. Places data for each node created. (You'll probably want
        to use the consumer_id for this, just for accounting purposes -- it's 
        not used as part of the balancing algorithm).

        Our zc.zk.ZooKeeper object doesn't know how to create nodes, so we
        have to dig into the zookeeper base library. Note that we can't create
        all of it in one go -- ZooKeeper only allows atomic operations, and
        so we're creating these one at a time.
        """
        if not path.startswith("/"):
            raise ValueError("Paths must be fully qualified (begin with '/').")

        def _build_path(nodes):
            return "/" + "/".join(nodes)

        nodes_to_create = path[1:].split("/") # remove beginning "/"
        created_so_far = []
        for node in nodes_to_create:
            created_path = _build_path(created_so_far)
            print created_path
            if node and node not in self._zk.children(created_path).data:
                node_to_create = _build_path(created_so_far + [node])
                # If data is a string, we'll initialize the node with it...
                if isinstance(data, basestring):
                    init_data = data 
                else:
                    init_data = json.dumps(data)
                zookeeper.create(self._zk.handle, node_to_create, init_data, ZKUtil.ACL)

            created_so_far.append(node)

    def _path_for_topics(self):
        return "/brokers/topics"

    def _path_for_topic(self, topic):
        return "{0}/{1}".format(self._path_for_topics(), topic)

    def _path_for_consumer_ids(self, consumer_group):
        return u"/consumers/{0}/ids".format(consumer_group)

    def _path_for_consumer_id(self, consumer_group, consumer_id):
        return u"{0}/{1}".format(_path_for_consumer_ids(consumer_group),
                                 consumer_id)

    def _zk_properties(self, path):
        return self._zk.properties(path).data['string_value']

    def _zk_children(self, path):
        return self._zk.children(path).data


class ZKProducer(object):

    def __init__(self, zk_conn_str, topic):
        self._id = uuid.uuid1()
        self._topic = topic
        self._zk_util = ZKUtil(zk_conn_str)

        # Try to pull the brokers and partitions we can send to on this topic
        self._broker_partitions = self._zk_util.broker_partitions_for(self.topic)
        if not self._broker_partitions:
            raise NoAvailablePartitionsError(
                u"No brokers were initialized for topic {0}".format(self.topic))

        # This will collapse duplicates so we only have one conn per host/port
        broker_conn_info = frozenset((bp.broker_id, bp.host, bp.port)
                                     for bp in self._broker_partitions)
        self._connections = dict((broker_id, Kafka(host, port))
                                 for broker_id, host, port in broker_conn_info)

    @property
    def topic(self):
        return self._topic
    
    @property
    def broker_partitions(self):
        return self._broker_partitions[:]

    # FIXME: Change this behavior so that it's random if they don't specify
    #        an explicit key.
    def send(self, msgs, key=hash):
        """key can either be a function that takes msgs as an arg and returns
        a hash number, or it can be an object that Python's hash() will work 
        on."""
        if not msgs:
            return
        broker_partition = self._broker_partition_for_msgs(msgs, key)
        kafka_conn = self._connections[broker_partition.broker_id]
        kafka_conn.produce(self.topic, msgs, broker_partition.partition)

        bytes_sent = sum(len(m) for m in msgs)
        log.debug(self._log_str(u"sent {0} bytes to {1}"
                                .format(bytes_sent, broker_partition)))
        return broker_partition
    
    def _broker_partition_for_msgs(self, msgs, key=hash):
        if callable(key): # it's a function to call on msg to determine a hash
            target_index = key(msgs[0]) % len(self._broker_partitions)
        else: # they just passed some number or tuple and want us to hash it
            target_index = hash(key) % len(self._broker_partitions)
        return self._broker_partitions[target_index]

    def _log_str(self, s):
        return u"ZKProducer {0} > {1}".format(self._id, s)


class Consumer(object):
    """Take 2 on the rebalancing code."""

    def __init__(self, zk_conn, consumer_group, topic):
        self._id = self._create_consumer_id(consumer_group)
        self._topic = topic
        self._zk_util = ZKUtil(zk_conn)
        self._consumer_group = consumer_group

        self._register()
        self._rebalance()

    @property
    def id(self): return self._id
    @property
    def topic(self): return self._topic
    @property
    def consumer_group(self): return self._consumer_group

    def _create_consumer_id(self, consumer_group_id):
        """Create a Consumer ID in the same way Kafka's reference client does"""
        hostname = platform.node()
        ms_since_epoch = int(time() * 1000)
        uuid_top_hex = uuid.uuid4().hex[:8]
        consumer_uuid = "{0}-{1}-{2}".format(hostname, ms_since_epoch, uuid_top_hex)

        return "{0}_{1}".format(consumer_group_id, consumer_uuid)

    def _register(self):
        """Register ourselves as a consumer in this consumer_group"""
        self._zk_util.register_consumer(self.consumer_group, self.id, self.topic)

    def _rebalance(self):
        # Grab all possible partitions that we could send to...       
        all_partitions = self._zk_util.broker_partitions_for(self.topic)

        # Grab all consumers
        all_consumers = self._zk_util.consumer_ids_for(self.topic)






































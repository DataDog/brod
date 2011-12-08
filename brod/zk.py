"""
TODO:
* Move producer, consumer into their own files
* MessageSet and FetchResult need to go deeper into the stack in base
* ZKUtil should be broken up into two pieces -- one with path info, the rest 
  to get folded into consumer and producer as necessary.
"""

import json
import logging
import platform
import time
import uuid
from collections import namedtuple, Mapping
from itertools import chain

import zookeeper
from zc.zk import ZooKeeper

from brod.base import KafkaError
from brod.blocking import Kafka

log = logging.getLogger('brod.zk')

class NoAvailablePartitionsError(KafkaError): pass
class ConsumerEntryNotFoundError(KafkaError): pass

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


class FetchResult(object):
    """A FetchResult is what's returned when we do a MULTIFETCH request. It 
    can contain an arbitrary number of message sets, which it'll eventually
    be able to query more intelligently than this. :-P

    This should eventually move to base and be returned in a multifetch()
    """

    def __init__(self, message_sets):
        self._message_sets = message_sets[:]

    def __iter__(self):
        return iter(self._message_sets)
    
    def __len__(self):
        return len(self._message_sets)

    def __getitem__(self, i):
        return self._message_sets[i]
    
    @property
    def broker_partitions(self):
        return [msg_set.broker_partition for msg_set in self]


class MessageSet(object):
    """A collection of messages and offsets returned from a request made to
    a single broker/topic/partition. Allows you to iterate via (offset, msg)
    tuples and grab origin information.

    ZK info might not be available if this came from a regular multifetch. This
    should be moved to base.
    """
    def __init__(self, broker_partition, offsets_msgs):
        self._offsets_msgs = offsets_msgs[:]
        self._broker_partition = broker_partition
    
    ################## Where did I come from? ##################
    @property
    def broker_partition(self):
        return self._broker_partition

    @property
    def topic(self):
        return self.broker_partition.topic

    ################## What do I have inside? ##################
    @property
    def offsets(self):
        return [offset for offset, msg in self]

    @property
    def messages(self):
        return [msg for offset, msg in self]

    @property
    def start_offset(self):
        return self.offsets[0] if self else None
    
    @property
    def end_offset(self):
        return self.offsets[-1] if self else None

    @property
    def next_offset(self):
        # FIXME FIXME FIXME: This calcuation should be done at a much deeper
        # level, or else this won't work with compressed messages, or be able
        # to detect the difference between 0.6 and 0.7 headers
        if not self:
            return None

        MESSAGE_HEADER_SIZE = 10
        last_offset, last_msg = self._offsets_msgs[-1]
        next_offset = last_offset + len(last_msg) + MESSAGE_HEADER_SIZE
        return next_offset

    @property
    def size(self):
        return sum(len(msg) for msg in self.messages)

    def __iter__(self):
        return iter(self._offsets_msgs)
    
    def __len__(self):
        return len(self._offsets_msgs)

    def __cmp__(self, other):
        bp_cmp = cmp(self.broker_partition, other.broker_partition)
        if bp_cmp:
            return bp_cmp
        else:
            return cmp(self._offsets_msgs, other.offsets_msgs)

    def __unicode__(self):
        return "Broker Partition: {0}\nContents: {1}".format(self.broker_partition, self._offsets_msgs)

    ################## Parse from binary ##################
    @classmethod
    def parse(self, broker_partition, data):
        raise NotImplementedError()


class ZKUtil(object):

    # This is a free-for-all ACL that we should re-evaluate later
    ACL = [{"perms": 0x1f, "scheme": "world", "id": "anyone"}]

    """Abstracts all Kafka-specific ZooKeeper access."""
    def __init__(self, zk_conn_str):
        self._zk = ZooKeeper(zk_conn_str)
    
    def close(self):
        self._zk.close()

    def broker_partitions_for(self, topic):
        """Return a list of BrokerPartitions based on values found in 
        ZooKeeper."""
        # Get the broker_ids first...
        broker_ids = self.broker_ids_for(topic)
        # log.debug(u"broker_ids: {0}".format(broker_ids))

        # Then the broker_strings for each broker
        broker_paths = map(self.path_for_broker, broker_ids)
        # log.debug(u"broker_paths: {0}".format(broker_paths))

        broker_strings = map(self._zk_properties, broker_paths)
        # log.debug(u"broker_strings: {0}".format(broker_strings))

        # Then the num_parts per broker (each could be set differently)
        broker_topic_paths = [self.path_for_broker_topic(broker_id, topic) 
                              for broker_id in broker_ids]
        num_parts = map(self._zk_properties, broker_topic_paths)
        # log.debug(u"num_parts: {0}".format(num_parts))
        
        # BrokerPartition
        return list(
                   chain.from_iterable(
                       BrokerPartition.from_zk(broker_id, broker_string, topic, n)
                       for broker_id, broker_string, n
                       in zip(broker_ids, broker_strings, num_parts)
                   )
               )

    def broker_ids_for(self, topic):
        topic_path = self.path_for_topic(topic)
        try:
            topic_node_children = self._zk_children(topic_path)
        except zookeeper.NoNodeException:
            log.warn(u"Couldn't find {0} - No brokers have topic {1} yet?"
                     .format(topic_path, topic))
            return []

        return sorted(int(broker_id) for broker_id in topic_node_children)

    def consumer_ids_for(self, topic, consumer_group):
        """For a given consumer group, return a list of all consumer_ids that
        are currently registered in that group."""
        # Gets the ids node below which are all consumer_ids in this group
        cg_path = self.path_for_consumer_ids(consumer_group)

        # All consumer_ids for this group, but not all of them are necessarily
        # interested in our topic
        consumer_ids_in_group = sorted(self._zk_children(cg_path))
        consumer_id_paths = [self.path_for_consumer_id(consumer_group, consumer_id)
                             for consumer_id in consumer_ids_in_group]
        consumer_id_data = [self._zk_properties(path) 
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
        self._create_path_if_needed(self.path_for_consumer_ids(consumer_group),
                                    consumer_id)
        # Create an emphermal node for this consumer
        consumer_id_path = self.path_for_consumer_id(consumer_group, consumer_id)
        log.info("Registering Consumer {0}, trying to create {1}"
                 .format(consumer_id, consumer_id_path))
        zookeeper.create(self._zk.handle, 
                         consumer_id_path,
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
            if node and node not in self._zk.children(created_path).data:
                node_to_create = _build_path(created_so_far + [node])
                # If data is a string, we'll initialize the node with it...
                if isinstance(data, basestring):
                    init_data = data 
                else:
                    init_data = json.dumps(data)
                zookeeper.create(self._zk.handle, node_to_create, init_data, ZKUtil.ACL)
            created_so_far.append(node)

    def offsets_for(self, consumer_group, consumer_id, broker_partitions):
        """Return a dictionary mapping broker_partitions to offsets."""
        UNKNOWN_OFFSET = 2**63 - 1 # Long.MAX_VALUE, it's what Kafka's client does
        bps_to_offsets = {}

        for bp in broker_partitions:
            # The topic might not exist at all, in which case no broker has 
            # anything, so there's no point in making the offsets nodes and such
            if self._zk.exists(self.path_for_topic(bp.topic)):
                offset_path = self.path_for_offset(consumer_group, 
                                                   bp.topic, 
                                                   bp.broker_id,
                                                   bp.partition)
                try:
                    offset = int(self._zk_properties(offset_path))
                except zookeeper.NoNodeException as ex:
                    # This is counter to the Kafka client behavior, put here for
                    # simplicity for now. FIXME: Dave
                    self._create_path_if_needed(offset_path, 0)
                    offset = 0

                bps_to_offsets[bp] = offset
        
        return bps_to_offsets

    def save_offsets_for(self, consumer_group, bps_to_next_offsets):
        log.debug("Saving offsets {0}".format(bps_to_next_offsets.values()))
        for bp, next_offset in sorted(bps_to_next_offsets.items()):
            # The topic might not exist at all, in which case no broker has 
            # anything, so there's no point in making the offsets nodes and such
            if self._zk.exists(self.path_for_topic(bp.topic)):
                for bp, next_offset in bps_to_next_offsets.items():
                    offset_path = self.path_for_offset(consumer_group, 
                                                       bp.topic, 
                                                       bp.broker_id,
                                                       bp.partition)
                try:
                    offset_node = self._zk.properties(offset_path)
                except zookeeper.NoNodeException as ex:
                    self._create_path_if_needed(offset_path, bps)
                    offset_node = self._zk.properties(offset_path)
                    next_offset = 0 # If we're creating the node now, assume we
                                    # need to start at 0.                
                # None is the default value when we don't know what the next
                # offset is, possibly because the MessageSet is empty...
                if next_offset is not None:
                    offset_node.set(string_data=str(next_offset))

    def path_for_broker_topic(self, broker_id, topic_name):
        return "{0}/{1}".format(self.path_for_topic(topic_name), broker_id)

    def path_for_brokers(self):
        return "/brokers/ids"

    def path_for_broker(self, broker_id):
        return "/brokers/ids/{0}".format(broker_id)

    def path_for_topics(self):
        return "/brokers/topics"

    def path_for_topic(self, topic):
        return "{0}/{1}".format(self.path_for_topics(), topic)
    
    def path_for_offsets(self, consumer_group, topic):
        return ("/consumers/{0}/offsets/{1}".format(consumer_group, topic))

    def path_for_offset(self, consumer_group, topic, broker_id, partition):
        path_for_offsets = self.path_for_offsets(consumer_group, topic)
        return "{0}/{1}-{2}".format(path_for_offsets, broker_id, partition)

    def path_for_consumer_ids(self, consumer_group):
        return u"/consumers/{0}/ids".format(consumer_group)

    def path_for_consumer_id(self, consumer_group, consumer_id):
        return u"{0}/{1}".format(self.path_for_consumer_ids(consumer_group),
                                 consumer_id)

    def _zk_properties(self, path):
        node_data = self._zk.properties(path).data
        if 'string_value' in node_data:
            return node_data['string_value']
        return node_data

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

    def close(self):
        self._zk_util.close()

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

    def __del__(self):
        self.close()


class ZKConsumer(object):
    """Take 2 on the rebalancing code."""

    def __init__(self, zk_conn, consumer_group, topic):
        self._id = self._create_consumer_id(consumer_group)
        self._topic = topic
        self._zk_util = ZKUtil(zk_conn)
        self._consumer_group = consumer_group
        self._needs_rebalance = True
        self._broker_partitions = [] # This gets updated during rebalancing

        self._register()

        self._topic_watch = None
        self._topics_watch = None
        self._consumers_watch = None
        self._brokers_watch = None

        self.rebalance()

    @property
    def id(self): return self._id
    @property
    def topic(self): return self._topic
    @property
    def consumer_group(self): return self._consumer_group

    @property
    def broker_partitions(self):
        if self._needs_rebalance:
            self.rebalance()
        return self._broker_partitions

    @property
    def brokers(self):
        return sorted(frozenset(bp.broker_id for bp in self.broker_partitions))

    def close(self):
        self._zk_util.close()

    def fetch(self, max_size=None):
        log.debug("Fetch called on Consumer {0}".format(self._id))
        if self._needs_rebalance:
            self.rebalance()

        # Find where we're starting from
        bps_to_offsets = self._zk_util.offsets_for(self.consumer_group,
                                                   self._id,
                                                   self.broker_partitions)
        
        # Do all the fetches we need to (this should get replaced with 
        # multifetch or performance is going to suck wind later)...
        message_sets = []
        # We only iterate over those broker partitions for which we have offsets
        for bp in bps_to_offsets:
            offset = bps_to_offsets[bp]
            kafka = self._connections[bp.broker_id]

            if offset is None:
                partition = kafka.partition(bp.topic, bp.partition)
                offset = partition.latest_offset()

            offsets_msgs = kafka.fetch(bp.topic, 
                                       offset,
                                       partition=bp.partition,
                                       max_size=max_size)
            message_sets.append(MessageSet(bp, offsets_msgs))
        
        result = FetchResult(sorted(message_sets))
        bps_to_next_offsets = dict((msg_set.broker_partition, msg_set.next_offset)
                                   for msg_set in result)

        # Now persist our new offsets
        self._zk_util.save_offsets_for(self.consumer_group, bps_to_next_offsets)

        return result

    def _create_consumer_id(self, consumer_group_id):
        """Create a Consumer ID in the same way Kafka's reference client does"""
        hostname = platform.node()
        ms_since_epoch = int(time.time() * 1000)
        uuid_top_hex = uuid.uuid4().hex[:8]
        consumer_uuid = "{0}-{1}-{2}".format(hostname, ms_since_epoch, uuid_top_hex)

        return "{0}_{1}".format(consumer_group_id, consumer_uuid)

    def _register(self):
        """Register ourselves as a consumer in this consumer_group"""
        self._zk_util.register_consumer(self.consumer_group, self.id, self.topic)
        # self._zk_util.create_path_if_needed()

    def rebalance(self):
        """Rebalancing algorithm is slightly different from that described in
        the design doc (mostly in the sense that the design doc algorithm will
        leave partitions unassigned if there's an uneven distributions). The 
        idea is that we split the partitions as evently as possible, and if
        some consumers need to have more partitions than others, the extra 
        partitions always go to the earlier consumers in the list. So you could
        have a distribution like 4-4-4-4 or 5-5-4-4, but never 4-4-4-5.
        """
        log.info(("Rebalance triggered for Consumer {0}, broker partitions " + \
                  "before rebalance: {1}").format(self.id, self._broker_partitions))
        # Get all the consumer_ids in my consumer_group who are listening to 
        # this topic (this includes us).
        all_topic_consumers = self._zk_util.consumer_ids_for(self.topic, 
                                                             self.consumer_group)
        # Where do I rank in the consumer_group list?
        all_broker_partitions = self._zk_util.broker_partitions_for(self.topic)
        try:
            my_index = all_topic_consumers.index(self.id)
        except ValueError:
            msg_tmpl = "This consumer ({0}) not found list of consumers " +\
                       "for this topic {1}: {2}"
            raise ConsumerEntryNotFoundError(
                msg_tmpl.format(self.id, self.topic, all_topic_consumers))

        bp_per_consumer = len(all_broker_partitions) / len(all_topic_consumers)
        consumers_with_extra = range(len(all_broker_partitions) % len(all_topic_consumers))

        # If the current consumer is among those that have an extra partition...
        num_parts = bp_per_consumer + (1 if my_index in consumers_with_extra else 0)

        # If the previous consumer was among the those that have an extra 
        # partition, add my_index to account for the extra partitions
        start = my_index * bp_per_consumer + \
                (my_index if my_index - 1 in consumers_with_extra else 0)

        ############## Set our state info... ##############
        self._broker_partitions = all_broker_partitions[start:start+num_parts]

        # This will collapse duplicates so we only have one conn per host/port
        broker_conn_info = frozenset((bp.broker_id, bp.host, bp.port)
                                     for bp in self._broker_partitions)
        self._connections = dict((broker_id, Kafka(host, port))
                                 for broker_id, host, port in broker_conn_info)

        # Register all our callbacks so we know when to do this next
        self._register_callbacks()
        if self._all_callbacks_registered():
            self._needs_rebalance = False
        
        # Report our progress
        log.info("Rebalance finished for Consumer {0}: {1}".format(self.id, unicode(self)))


    def _unbalance(self, nodes):
        """We use this so that rebalancing can happen at specific points (like
        before we make a new fetch)"""
        self._needs_rebalance = True

    def _all_callbacks_registered(self):
        """Are all the callbacks we need to know when to rebalance actually 
        registered? Some of these (like the topic ones) are the responsibility
        of the broker to create. If they're not all registered yet, we need 
        to be paranoid about rebalancing."""
        return all([self._consumers_watch, 
                    self._brokers_watch,
                    self._brokers_watch,
                    self._topic_watch])

    def _register_callbacks(self):
        zk = self._zk_util._zk # FIXME: Evil breaking of encapsulation

        # All this if None nonsense is there because some of these nodes
        # need to be created by the broker but won't be until the topic is 
        # created.
        path_for_consumers = self._zk_util.path_for_consumer_ids(self.consumer_group)
        path_for_brokers = self._zk_util.path_for_brokers()
        path_for_topics = self._zk_util.path_for_topics()
        path_for_topic = self._zk_util.path_for_topic(self.topic)
        if self._consumers_watch is None and zk.exists(path_for_consumers):
            self._consumers_watch = zk.children(path_for_consumers)(self._unbalance)
        if self._brokers_watch is None and zk.exists(path_for_brokers):
            self._brokers_watch = zk.children(path_for_brokers)(self._unbalance)
        if self._topics_watch is None and zk.exists(path_for_topics):
            self._topics_watch = zk.children(path_for_topics)(self._unbalance)
        if self._topic_watch is None and zk.exists(path_for_topic):
            self._topic_watch = zk.children(path_for_topic)(self._unbalance)

        log.debug("Consumer {0} has watches: {1}"
                  .format(self._id, sorted(zk.watches.data.keys())))

    def __unicode__(self):
        return ("ZKConsumer {0} attached to broker partitions {1}"
                .format(self.id, self._broker_partitions))

    def __del__(self):
        self.close()




























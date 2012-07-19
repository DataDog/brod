"""
TODO:
* Move producer, consumer into their own files
* ZKUtil should be broken up into two pieces -- one with path info, the rest 
  to get folded into consumer and producer as necessary.

FIXME: Extract the logic for testing how the brokers get partitioned so we can
       test them in simple unit tests without involving Kafka/ZK.
"""
import json
import logging
import platform
import random
import time
import uuid
from collections import namedtuple, Mapping, defaultdict
from itertools import chain

import zookeeper
from zc.zk import ZooKeeper, FailedConnect

from brod.base import BrokerPartition, ConsumerStats, MessageSet
from brod.base import ConnectionFailure, FetchResult, KafkaError, OffsetOutOfRange
from brod.simple import SimpleConsumer
from brod.blocking import Kafka

log = logging.getLogger('brod.zk')

class NoAvailablePartitionsError(KafkaError): pass
class ConsumerEntryNotFoundError(KafkaError): pass
class ZKConnectError(KafkaError): pass

class ZKUtil(object):

    # This is a free-for-all ACL that we should re-evaluate later
    ACL = [{"perms": 0x1f, "scheme": "world", "id": "anyone"}]

    """Abstracts all Kafka-specific ZooKeeper access."""
    def __init__(self, zk_conn_str, zk_timeout=None):
        try:
            self._zk = ZooKeeper(zk_conn_str, zk_timeout)
        except FailedConnect as e:
            raise ZKConnectError(e)
    
    def close(self):
        self._zk.close()

    def broker_partitions_for(self, topic, force_partition_zero=False):
        """Return a list of BrokerPartitions based on values found in 
        ZooKeeper.

        If you set force_partition_zero=True, we will always return partition 
        0 of a broker, even if no topic has been created for it yet. Consumers
        don't need this, because it means there's nothing there to read anyway.
        But Producers need to be bootstrapped with it.
        """
        # Get the broker_ids first...
        if force_partition_zero:
            broker_ids = self.all_broker_ids()
        else:
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
        
        # Every broker has at least one partition, even if it's not published
        num_parts = []
        for p in broker_topic_paths:
            broker_parts = self._zk_properties(p) if self._zk.exists(p) else 1
            num_parts.append(broker_parts)
        # log.debug(u"num_parts: {0}".format(num_parts))
        
        # BrokerPartition
        return list(
                   chain.from_iterable(
                       BrokerPartition.from_zk(broker_id, broker_string, topic, n)
                       for broker_id, broker_string, n
                       in zip(broker_ids, broker_strings, num_parts)
                   )
               )


    def offsets_state(self, consumer_group):
        """For a given consumer_group, get back a ZK state dict that looks like:
        
        {
            "topic1" : {
                "1-0" : 1002830,
                "1-1" : 1201221,  
                "2-0" : 3232445,
                "2-1" : 2309495
            }
            "topic2" : {
                "1-0" : 1002830,
                "1-1" : 1201221,  
                "2-0" : 3232445,
                "2-1" : 2309495
            }
        }

        Keys are topic names for that consumer group, and the values are the 
        children in ZooKeeper -- other dicts with keys that are 
        {broker_id}-{partition_id} to offsets.
        """
        def topic_offsets(topic):
            broker_parts_path = self.path_for_offsets(consumer_group, topic)
            broker_parts = self._zk_children(broker_parts_path)
            return dict((bp, int(self._zk_properties(broker_parts_path + "/" + bp)))
                        for bp in broker_parts)

        state = {}
        topics = self._zk_children("/consumers/{0}/offsets".format(consumer_group))
        for topic in topics:
            state[topic] = topic_offsets(topic)
        
        return state

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
        bp_ids_to_offsets = sorted((bp.id, offset) 
                                   for bp, offset in bps_to_next_offsets.items())
        log.debug("Saving offsets {0}".format(bp_ids_to_offsets))
        for bp, next_offset in sorted(bps_to_next_offsets.items()):
            # The topic might not exist at all, in which case no broker has 
            # anything, so there's no point in making the offsets nodes and such
            if self._zk.exists(self.path_for_topic(bp.topic)):
                offset_path = self.path_for_offset(consumer_group, 
                                                   bp.topic, 
                                                   bp.broker_id,
                                                   bp.partition)
                try:
                    offset_node = self._zk.properties(offset_path)
                except zookeeper.NoNodeException as ex:
                    self._create_path_if_needed(offset_path, bp)
                    offset_node = self._zk.properties(offset_path)
                    next_offset = 0 # If we're creating the node now, assume we
                                    # need to start at 0.                
                # None is the default value when we don't know what the next
                # offset is, possibly because the MessageSet is empty...
                if next_offset is not None:
                    log.debug("Node %s: setting to %s" % (offset_node, next_offset))
                    offset_node.set(string_value=str(next_offset))
 
    def broker_ids_for(self, topic):
        topic_path = self.path_for_topic(topic)
        try:
            topic_node_children = self._zk_children(topic_path)
        except zookeeper.NoNodeException:
            log.warn(u"Couldn't find {0} - No brokers have topic {1} yet?"
                     .format(topic_path, topic))
            return []

        return sorted(int(broker_id) for broker_id in topic_node_children)

    def all_broker_ids(self):
        brokers_path = self.path_for_brokers()
        try:
            topic_node_children = self._zk_children(brokers_path)
        except zookeeper.NoNodeException:
            log.error(u"Couldn't find brokers entry {0}".format(brokers_path))
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

    def __init__(self, zk_conn_str, topic, zk_timeout=None):
        self._id = uuid.uuid1()
        self._topic = topic
        self._bps_changed = False
        self._zk_util = ZKUtil(zk_conn_str, zk_timeout)

        # Try to pull the brokers and partitions we can send to on this topic
        self._brokers_watch = None
        self._topic_watch = None
        self._register_callbacks()
        self.detect_broker_partitions()

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

    def send(self, msgs):
        if not msgs:
            return

        if not self._all_callbacks_registered():
            self._register_callbacks()
        if self._bps_changed:
            self.detect_broker_partitions()

        broker_partition = random.choice(self._broker_partitions)
        kafka_conn = self._connections[broker_partition.broker_id]
        kafka_conn.produce(self.topic, msgs, broker_partition.partition)

        bytes_sent = sum(len(m) for m in msgs)
        log.debug(self._log_str(u"sent {0} bytes to {1}"
                                .format(bytes_sent, broker_partition)))
        return broker_partition

    def detect_broker_partitions(self):
        bps = self._zk_util.broker_partitions_for(self.topic, 
                                                  force_partition_zero=True)
        if not bps:
            raise NoAvailablePartitionsError(u"No brokers were found!")
        
        self._broker_partitions = bps
        self._bps_changed = False

    def _unbalance(self, nodes):
        self._bps_changed = True

    def _register_callbacks(self):
        zk = self._zk_util._zk # FIXME: Evil breaking of encapsulation

        path_for_brokers = self._zk_util.path_for_brokers()
        path_for_topic = self._zk_util.path_for_topic(self.topic)
        if self._brokers_watch is None and zk.exists(path_for_brokers):
            self._brokers_watch = zk.children(path_for_brokers)(self._unbalance)
        if self._topic_watch is None and zk.exists(path_for_topic):
            self._topic_watch = zk.children(path_for_topic)(self._unbalance)

        log.debug("Producer {0} has watches: {1}"
                  .format(self._id, sorted(zk.watches.data.keys())))

    def _all_callbacks_registered(self):
        """Are all the callbacks we need to know when to rebalance actually 
        registered? Some of these (like the topic ones) are the responsibility
        of the broker to create."""
        return all([self._brokers_watch, self._topic_watch])

    def _log_str(self, s):
        return u"ZKProducer {0} > {1}".format(self._id, s)

    def __del__(self):
        self.close()


class ZKConsumer(object):
    """Take 2 on the rebalancing code."""

    def __init__(self, zk_conn, consumer_group, topic, autocommit=True, zk_timeout=None):
        """FIXME: switch arg order and default zk_conn to localhost?"""
        # Simple attributes we return as properties
        self._id = self._create_consumer_id(consumer_group)
        self._topic = topic
        self._consumer_group = consumer_group
        self._autocommit = autocommit

        # Internal vars
        self._zk_util = ZKUtil(zk_conn, zk_timeout)
        self._needs_rebalance = True
        self._broker_partitions = [] # Updated during rebalancing
        self._bps_to_next_offsets = {} # Updated after a successful fetch
        self._rebalance_enabled = True # Only used for debugging purposes

        # These are to handle ZooKeeper notification subscriptions.
        self._topic_watch = None
        self._topics_watch = None
        self._consumers_watch = None
        self._brokers_watch = None

        # Register ourselves with ZK so other Consumers know we're active.
        self._register()

        # Force a rebalance so we know which broker-partitions we own
        self.rebalance()

        self._stats = defaultdict(lambda: ConsumerStats(fetches=0, bytes=0, messages=0, max_fetch=0))

    @property
    def id(self): return self._id
    @property
    def topic(self): return self._topic
    @property
    def consumer_group(self): return self._consumer_group
    @property
    def autocommit(self): return self._autocommit


    @property
    def stats(self): 
        ''' Returns the aggregate of the stats from all the broker partitions
        '''
        fetches = 0
        bytes = 0
        messages = 0
        max_fetch = 0
        for stats in self._stats.values():
            fetches += stats.fetches
            bytes += stats.bytes
            messages += stats.messages
            max_fetch = max(max_fetch, stats.max_fetch)

        return ConsumerStats(fetches, bytes, messages, max_fetch)

    def stats_by_broker_partition(self):
        return dict(self._stats)

    @property
    def broker_partitions(self):
        if self._needs_rebalance:
            self.rebalance()
        return self._broker_partitions

    @property
    def brokers(self):
        return sorted(frozenset(bp.broker_id for bp in self.broker_partitions))

    def close(self):
        if hasattr(self, '_zk_util'):
            self._zk_util.close()
    
    def simple_consumer(self, bp_ids_to_offsets):
        """bp_pairs_to_offsets is a dictionary of tuples to integers like the
        following:

        {
            "0-0" : 2038903,
            "0-1" : 3930198,
            "1-0" : 3932088,
            "1-1" : 958
        }

        The keys are of the format "[broker_id]-[partition_id]".

        The values are offsets.

        This method will return a SimpleConsumer that is initialied to read from
        the brokers listed at the offsets specified.
        """
        all_broker_partitions = self._zk_util.broker_partitions_for(self.topic)
        broker_partitions = dict((bp, bp_ids_to_offsets(bp.id))
                                 for bp in all_broker_partitions
                                 if bp.id in bp_ids_to_offsets)
        
        return SimpleConsumer(self.topic, broker_partitions)

    def fetch(self, max_size=None, retry_limit=3, ignore_failures=False):
        """Return a FetchResult, which can be iterated over as a list of 
        MessageSets. A MessageSet is returned for every broker partition that
        is successfully queried, even if that MessageSet is empty.

        FIXME: This is where the adjustment needs to happen. Regardless of 
        whether a rebalance has occurred or not, we can very easily see if we
        are still responsible for the same partitions as we were the last time
        we ran, and set self._bps_to_next_offsets --> we just need to check if
        it's not None and if we still have the same offsets, and adjust 
        accordingly.
        """
        def needs_offset_values_from_zk(bps_to_offsets):
            """We need to pull offset values from ZK if we have no 
            BrokerPartitions in our BPs -> Offsets mapping, or if some of those
            Offsets are unknown (None)"""
            return (not bps_to_offsets) or (None in bps_to_offsets.values())

        log.debug("Fetch called on ZKConsumer {0}".format(self.id))
        if self._needs_rebalance:
            self.rebalance()

        # Find where we're starting from. If we've already done a fetch, we use 
        # our internal value. This is also all we can do in the case where 
        # autocommit is off, since any value in ZK will be out of date.
        bps_to_offsets = dict(self._bps_to_next_offsets)
        offsets_pulled_from_zk = False

        if needs_offset_values_from_zk(bps_to_offsets):
            # We have some offsets, but we've been made responsible for new
            # BrokerPartitions that we need to lookup.
            if bps_to_offsets:
                bps_needing_offsets = [bp for bp, offset in bps_to_offsets.items() 
                                       if offset is None]
            # Otherwise, it's our first fetch, so we need everything
            else:
                bps_needing_offsets = self.broker_partitions

            bps_to_offsets.update(self._zk_util.offsets_for(self.consumer_group,
                                                            self._id,
                                                            bps_needing_offsets))
            offsets_pulled_from_zk = True

        # Do all the fetches we need to (this should get replaced with 
        # multifetch or performance is going to suck wind later)...
        message_sets = []
        # We only iterate over those broker partitions for which we have offsets
        for bp in bps_to_offsets:
            offset = bps_to_offsets[bp]
            kafka = self._connections[bp.broker_id]
            partition = kafka.partition(bp.topic, bp.partition)

            if offset is None:
                offset = partition.latest_offset()
            
            try:
                offsets_msgs = kafka.fetch(bp.topic, 
                                           offset,
                                           partition=bp.partition,
                                           max_size=max_size)

            # If our fetch fails because it's out of range, and the values came
            # from ZK originally (not our internal incrementing), we assume ZK
            # is somehow stale, so we just grab the latest and march on.
            except OffsetOutOfRange as ex:
                if offsets_pulled_from_zk:
                    log.error("Offset {0} from ZooKeeper is out of range for {1}"
                              .format(offset, bp))
                    offset = partition.latest_offset()
                    log.error("Retrying with offset {0} for {1}"
                              .format(offset, bp))
                    offsets_msgs = kafka.fetch(bp.topic, 
                                               offset,
                                               partition=bp.partition,
                                               max_size=max_size)
                else:
                    raise
            except KafkaError as k_err:
                if ignore_failures:
                    log.error("Ignoring failed fetch on {0}".format(bp))
                    log.exception(k_err)
                    continue
                else:
                    raise

            msg_set = MessageSet(bp, offset, offsets_msgs)

            # fetches bytes messages max_fetch
            old_stats = self._stats[bp]
            self._stats[bp] = ConsumerStats(fetches=old_stats.fetches + 1,
                                        bytes=old_stats.bytes + msg_set.size,
                                        messages=old_stats.messages + len(msg_set),
                                        max_fetch=max(old_stats.max_fetch, msg_set.size))

            message_sets.append(msg_set)
        
        result = FetchResult(sorted(message_sets))

        # Now persist our new offsets
        for msg_set in result:
            self._bps_to_next_offsets[msg_set.broker_partition] = msg_set.next_offset

        if self._autocommit:
            self.commit_offsets()

        return result

    def commit_offsets(self):
        if self._bps_to_next_offsets:
            self._zk_util.save_offsets_for(self.consumer_group, 
                                           self._bps_to_next_offsets)

    def poll(self,
             start_offsets=None,
             end_offsets=None,
             poll_interval=1,
             max_size=None,
             retry_limit=3):
        """FIXME: start/end, retry_limit"""
        while True:
            for msg_set in self.fetch(max_size=max_size):
                yield msg_set
            time.sleep(poll_interval)

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
        """Figure out which brokers and partitions we should be consuming from,
        based on the latest information about the other consumers and brokers
        that are present.

        We registered for notifications from ZooKeeper whenever a broker or 
        consumer enters or leaves the pool. But we usually only rebalance right
        before we're about to take an action like fetching.

        The rebalancing algorithm is slightly different from that described in
        the design doc (mostly in the sense that the design doc algorithm will
        leave partitions unassigned if there's an uneven distributions). The 
        idea is that we split the partitions as evently as possible, and if
        some consumers need to have more partitions than others, the extra 
        partitions always go to the earlier consumers in the list. So you could
        have a distribution like 4-4-4-4 or 5-5-4-4, but never 4-4-4-5.

        Rebalancing has special consequences if the Consumer is doing manual 
        commits (autocommit=False):

        1. This Consumer will keep using the in memory offset state for all 
           BrokerPartitions that it was already following before the rebalance.
        2. The offset state for any new BrokerPartitions that this Consumer is
           responsible for after the rebalance will be read from ZooKeeper.
        3. For those BrokerPartitions that this Consumer was reading but is no
           longer responsible for after the rebalance, the offset state is 
           simply discarded. It is not persisted to ZooKeeper.
        
        So there is no guarantee of single delivery in this circumstance. If 
        BrokerPartition 1-0 shifts ownership from Consumer A to Consumer B in 
        the rebalance, Consumer B will pick up from the last manual commit of 
        Consumer A -- *not* the offset that Consumer A was at when the rebalance
        was triggered.
        """
        log.info(("Rebalance triggered for Consumer {0}, broker partitions " + \
                  "before rebalance: {1}").format(self.id, unicode(self)))

        if not self._rebalance_enabled:
            log.info("Rebalancing disabled -- ignoring rebalance request")
            return 

        # Get all the consumer_ids in our consumer_group who are listening to 
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

        # We're the first index,
        if my_index == 0:
            start = 0
        # All the indexes before us where ones with extra consumers
        elif my_index - 1 in consumers_with_extra:
            start = my_index * (bp_per_consumer + 1)
        # There is 0 or more consumers with extra partitions followed by 
        # 1 or more partitions with no extra partitions
        else:
            start = (len(consumers_with_extra) * (bp_per_consumer + 1)) + \
                    ((my_index - len(consumers_with_extra)) * bp_per_consumer)

        ############## Set our state info... ##############
        self._broker_partitions = all_broker_partitions[start:start+num_parts]

        # We keep a mapping of BrokerPartitions to their offsets. We ditch those
        # BrokerPartitions we are no longer responsible for...
        for bp in self._bps_to_next_offsets.keys():
            if bp not in self._broker_partitions:
                del self._bps_to_next_offsets[bp]

        # We likewise add new BrokerPartitions we're responsible for to our 
        # BP->offset mapping, and set their offsets to None, to indicate that
        # we don't know, and we have to check ZK for them.
        for bp in self._broker_partitions:
            self._bps_to_next_offsets.setdefault(bp, None)

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

    def disable_rebalance(self):
        """For debugging purposes -- disable rebalancing so that we can more 
        easily test things like trying to request from a Kafka broker
        that's down. Normally, the rebalancing means that we should only 
        encounter these situations in race conditions.

        I cannot think of any reason you would want to do this in actual code.
        """
        self._rebalance_enabled = False

    def enable_rebalance(self):
        """For debugging purposes -- re-enable rebalancing so that we can more 
        easily test things like trying to request from a Kafka broker
        that's down. Normally, the rebalancing means that we should only 
        encounter these situations in race conditions.

        I cannot think of any reason you would want to do this in actual code.
        """
        self._rebalance_enabled = True

    def _unbalance(self, nodes):
        """We use this so that rebalancing can happen at specific points (like
        before we make a new fetch)."""
        self._needs_rebalance = True

    def _all_callbacks_registered(self):
        """Are all the callbacks we need to know when to rebalance actually 
        registered? Some of these (like the topic ones) are the responsibility
        of the broker to create. If they're not all registered yet, we need 
        to be paranoid about rebalancing."""
        return all([self._consumers_watch, 
                    self._brokers_watch,
                    self._topics_watch,
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
        bp_ids = [bp.id for bp in self._broker_partitions]
        return ("ZKConsumer {0} attached to broker partitions {1}"
                .format(self.id, bp_ids))

    def __del__(self):
        self.close()


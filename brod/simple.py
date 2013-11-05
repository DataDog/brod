"""
This holds our SimpleConsumer and SimpleProducer -- classes that interact with
Kafka but have no communication with ZooKeeper

Behaviors to support:

* Starting from a particular offset for each partition.
* starting from the beginning (or first available partition)
* starting from the last available partition
* Support multiple brokers
* Be able to be jump started from a ZKConsumer

"""
import logging
import time
from brod import Kafka
from brod.base import ConsumerStats, MessageSet, FetchResult
from collections import Mapping, defaultdict

log = logging.getLogger('brod.simple')

class SimpleConsumer(object):
    """We'll make more convenient constructors later..."""

    def __init__(self, topic, broker_partitions, end_broker_partitions=None):
        """If broker_partitions is a list of BrokerPartitions, we assume that
        we'll start at the latest offset. If broker_partitions is a mapping of
        BrokerPartitions to offsets, we'll start at those offsets."""
        self._topic = topic
        self._broker_partitions = sorted(broker_partitions)
        self._stats = defaultdict(lambda: ConsumerStats(fetches=0, bytes=0, messages=0, max_fetch=0))
        self._bps_to_next_offsets = broker_partitions

        # This will collapse duplicaets so we only have one conn per host/port
        broker_conn_info = frozenset((bp.broker_id, bp.host, bp.port)
                                     for bp in self._broker_partitions)
        self._connections = dict((broker_id, Kafka(host, port))
                                 for broker_id, host, port in broker_conn_info)

        # Figure out where we're going to start from...
        if isinstance(broker_partitions, Mapping):
            self._bps_to_next_offsets = broker_partitions
        else:
            self._bps_to_next_offsets = dict((bp, self._connections[bp].latest_offset(bp.topic, bp.partition))
                                             for bp in broker_partitions)

        self._end_broker_partitions = end_broker_partitions or {}

    @property
    def id(self): return id(self)
    @property
    def topic(self): return self._topic
    @property
    def consumer_group(self): return None
    @property
    def autocommit(self): return False
    @property
    def broker_partitions(self): return self._broker_partitions[:]
    @property
    def brokers(self):
        return sorted(frozenset(bp.broker_id for bp in self._broker_partitions))

    def close(self):
        pass

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

    def commit_offsets(self):
        """This isn't supported for SimpleConsumers, but is present so that you
        don't have to change code when swapping a SimpleConsumer for a 
        ZKConsumer."""
        pass

    def fetch(self, max_size=None, min_size=None, fetch_step=None):
        log.debug("Fetch called on SimpleConsumer {0}".format(self.id))
        bps_to_offsets = self._bps_to_next_offsets
        
        # Do all the fetches we need to (this should get replaced with 
        # multifetch or performance is going to suck wind later)...
        message_sets = []
        # We only iterate over those broker partitions for which we have offsets
        for bp in bps_to_offsets:
            offset = bps_to_offsets[bp]
            kafka = self._connections[bp.broker_id]

            offsets_msgs = kafka.fetch(bp.topic, 
                                       offset,
                                       partition=bp.partition,
                                       min_size=min_size,
                                       max_size=max_size,
                                       fetch_step=fetch_step)

            msg_set = MessageSet(bp, offset, offsets_msgs)

            # fetches bytes messages max_fetch
            old_stats = self._stats[bp]
            self._stats[bp] = ConsumerStats(fetches=old_stats.fetches + 1,
                                        bytes=old_stats.bytes + msg_set.size,
                                        messages=old_stats.messages + len(msg_set),
                                        max_fetch=max(old_stats.max_fetch, msg_set.size))

            message_sets.append(msg_set)
        
        if message_sets:
            result = FetchResult(sorted(message_sets))
        else:
            result = FetchResult([])

        # Filter out broker partitions whose end offsets we've exceeded
        self._bps_to_next_offsets = {}
        for msg_result in result:
            bp = msg_result.broker_partition
            next_offset = msg_result.next_offset
            end_offset = self._end_broker_partitions.get(bp, None)

            if end_offset is None or next_offset <= end_offset:
                self._bps_to_next_offsets[bp] = next_offset
        
        return result
    
    def poll(self,
             start_offsets=None,
             end_offsets=None,
             poll_interval=1,
             min_size=None,
             max_size=None,
             fetch_step=None,
             retry_limit=3):
        while self._bps_to_next_offsets:
            for msg_set in self.fetch(min_size=min_size, max_size=max_size,
                                      fetch_step=fetch_step):
                yield msg_set
            time.sleep(poll_interval)
    


# consumer = SimpleConsumer("topic", broker_list)

# zk_consumer = ZKConsumer(...)

# The var we pass in is { (0, 1) : offset }

# consumer = zk_consumer.simple_from_offsets(brokers)

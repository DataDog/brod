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
# from base import ConsumerState

from collections import Mapping

class SimpleConsumer(object):
    """We'll make more convenient constructors later..."""

    def __init__(self, topic, broker_partitions):
        """If broker_partitions is a list of BrokerPartitions, we assume that
        we'll start at the latest offset. If broker_partitions is a mapping of
        BrokerPartitions to offsets, we'll start at those offsets."""
        self._topic = topic
        self._broker_partitions = sorted(broker_partitions)
        self._stats = ConsumerStats(fetches=0, bytes=0, messages=0, max_fetch=0)
        self._bps_to_next_offsets = broker_partitions

        # This will collapse duplicates so we only have one conn per host/port
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

    @property
    def id(): return id(self)
    @property
    def topic(self): return self._topic
    @property
    def consumer_group(self): return None
    @property
    def autocommit(self): return False
    @property
    def stats(self): return self._stats
    @property
    def broker_partitions(self): return self._broker_partitions[:]
    @property
    def brokers(self):
        return sorted(frozenset(bp.broker_id for bp in self._broker_partitions))

    def close(self):
        pass

    def commit_offsets(self):
        """This isn't supported for SimpleConsumers, but is present so that you
        don't have to change code when swapping a SimpleConsumer for a 
        ZKConsumer."""
        pass

    def fetch(self, max_size=None):
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
                                       max_size=max_size)
            message_sets.append(MessageSet(bp, offset, offsets_msgs))
        
        result = FetchResult(sorted(message_sets))

        self._bps_to_next_offsets = dict((msg_set.broker_partition, msg_set.next_offset)
                                         for msg_set in result)
        old_stats = self._stats  # fetches bytes messages max_fetch
        self._stats = ConsumerStats(fetches=old_stats.fetches + 1,
                                    bytes=old_stats.bytes + result.num_bytes,
                                    messages=old_stats.messages + result.num_messages,
                                    max_fetch=max(old_stats.max_fetch, result.num_bytes))
        return result
    
    def poll(self,
             start_offsets=None,
             end_offsets=None,
             poll_interval=1,
             max_size=None,
             retry_limit=3):
        while True:
            for msg_set in self.fetch(max_size=max_size):
                yield msg_set
            time.sleep(poll_interval)
    
    def end_at(self, bps_to_offsets):
        """Don't consume beyond this point for the bps listed"""
        pass

# consumer = SimpleConsumer("topic", broker_list)

# zk_consumer = ZKConsumer(...)

# The var we pass in is { (0, 1) : offset }

# consumer = zk_consumer.simple_from_offsets(brokers)
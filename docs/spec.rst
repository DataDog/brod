Kafka Wire Format
=================

`Kafka <http://incubator.apache.org/kafka/>`_ is a remarkably elegant messaging 
server that we love using at Datadog. This is an attempt to document the wire 
format and implementation details, both for our own reference while building 
brod, and to encourage others to write clients in other languages. Much of this
document is based off of the Kafka wire format
`wiki entry <https://cwiki.apache.org/confluence/display/KAFKA/Wire+Format>`_
by Jeffrey Damick and Taylor Gautier.

This does not yet cover ZooKeeper integration, but we hope to add that shortly.

Ground Rules
------------

If you haven't read the `design doc <http://incubator.apache.org/kafka/design.html>`_,
read it. There are a few things that haven't been updated for v0.7 yet, but it's
a great overview of Kafka.

Some really high level takeaways to get started:

* Kafka stores messages on disk, in a series of large, append-only log files
  broken up into segments.
* Topics are referenced by name, and each topic can have multiple partitions
  (partitions are numbered, starting from 0).
* Topics can be created dynamically just by writing to it, but the max number of
  partitions for a topic is fixed and determined by broker configuration. You 
  must specify which partition of a topic you want to read from or write to.
* Kafka relies on smarter clients to keep bookkeeping. When requesting messages,
  the client has to specify what offset in the topic/partition it wants them 
  pulled from. The broker does not keep track of what the client has read. More
  advanced setups use ZooKeeper to help with this tracking, but that is 
  currently beyond the scope of this document.
* The protocol is a work in progress, and new point releases can introduce
  backwards incompatibile changes.
* The broker runs on port 9092 by default.


Basic Objects
-------------

Request Header
**************

All requests must start with the following header::
    
     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                       REQUEST_LENGTH                          |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |         REQUEST_TYPE          |        TOPIC_LENGTH           |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /                                                               /
    /                    TOPIC (variable length)                    /
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                           PARTITION                           |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    REQUEST_LENGTH = int(4) // Length in bytes of entire request (excluding this field)
    REQUEST_TYPE   = int(2) // See table below
    TOPIC_LENGTH   = int(2) // Length in bytes of the topic name

    TOPIC = String // Topic name, ASCII, not null terminated
                   // This becomes the name of a directory, so no chars that 
                   // would be illegal on the filesystem.

    PARTITION = int(4) // Partition to act on. Number of available partitions is 
                       // controlled by broker config. Partition numbering 
                       // starts at 0.

    ============  =====  =======================================================
    REQUEST_TYPE  VALUE  DEFINITION
    ============  =====  =======================================================
    PRODUCE         0    Send a group of messages to a topic and partition.
    FETCH           1    Fetch a group of messages from a topic and partition.
    MULTIFETCH      2    
    MULTIPRODUCE    3
    OFFSETS         4
    ============  =====  =======================================================


Response Header
***************

All responses have the following 6 byte header::
    
     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                        RESPONSE_LENGTH                        |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |         ERROR_CODE            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    ================  =====  ===================================================
    ERROR_CODE        VALUE  DEFINITION
    ================  =====  ===================================================
    OffsetOutOfRange    1    Offset requested is not in the 
    InvalidMessage      2
    WrongPartition      3    You tried to access a partition that doesn't exist.
    InvalidFetchSize    4    The size you requested for fetching is smaller than
                             the message you're trying to fetch.
    ================  =====  ===================================================

FIXME: Check these names against their source tree

FIXME: Check what happens when you have a partition that isn't created yet and
you try to read from it.

Message (<= v0.6)
**********************

Version 0.6 and earlier of Kafka use the following format::

     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                             LENGTH                            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |     MAGIC       |                   CHECKSUM                  |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    | CHECKSUM (cont.)|                    PAYLOAD                  /
    +-+-+-+-+-+-+-+-+-+                                             /
    /                         PAYLOAD (cont.)                       /
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    LENGTH   = int(4)  // Length in bytes of entire message (excluding this field)
    MAGIC    = byte(1) // 0 is the only valid value
    CHECKSUM = int(4)  // CRC32 checksum of the PAYLOAD
    PAYLOAD  = Bytes[] // Message content

The offsets to request messages are just byte offsets. To find the offset of the
next message, take the offset of this message (that you made in the request),
and add LENGTH + 4 bytes (length of this message + 4 byte header to represent
the length of this message).


Message (>= v0.7)
**********************

Starting with version 0.7, Kafka added an extra field for compression::

     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                             LENGTH                            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |     MAGIC       |  COMPRESSION  |           CHECKSUM          |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |      CHECKSUM (cont.)           |           PAYLOAD           /
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                             /
    /                         PAYLOAD (cont.)                       /
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    LENGTH = int(4) // Length in bytes of entire message (excluding this field)
    MAGIC = byte(1) // 0 = COMPRESSION attribute byte does not exist (v0.6 and below)
                    // 1 = COMPRESSION attribute byte exists (v0.7 and above)
    COMPRESSION = byte(1) // 0 = none; 1 = gzip; 2 = snappy;
                          // Only exists at all if MAGIC == 1
    CHECKSUM = int(4) // CRC32 checksum of the PAYLOAD
    PAYLOAD = Bytes[] // Message content

Note that compression is end-to-end. Meaning that the Producer is responsible
for sending the compressed payload, it's stored compressed on the broker, and
the Consumer is responsible for decompressing it. Gzip gives better compression
ratio, snappy gives faster performance.

Let's look at what compressed messages act like::

    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |          CM1         |         CM2        |         CM3         |
    | M1 | M2 | M3 | M4... | M12 | M13 | M14... | M26 | M27 | M28 ... |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

In this scenario, let's say that `M1`, `M2`, etc. represent complete,
*uncompressed* messages that the user of your library wants to send. What your
client needs to do is take `M1`, `M2`... up to some predetermined number,
concatenate them together, and then compress them using gzip or snappy. The
result (`CM1` in  this case) becomes the PAYLOAD for the *compressed* message
your library will send to Kafka.

It also means that we have to be careful about calculating the offsets. To
Kafka, `M1`, `M1`, don't really exist. It only sees the `CM1` you send. So when
you make calculations for the offset you can fetch next, you have to make sure
you're doing it on the boundaries of the compressed messages, not the inner
messages.


Interactions
------------

Produce
*******

To produce messages from the client and send to Kafka, use the following format::

     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /                       REQUEST HEADER (above)                  /
    /                                                               /
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                         MESSAGES_LENGTH                       |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /                                                               /
    /                           MESSAGES                            /
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    MESSAGES_LENGTH = int(4) // Length in bytes of the MESSAGES section
    MESSAGES = Collection of MESSAGES (see above)


There is no response to a PRODUCE Request. There is currently no way to tell
if the produce was successful or not. This is 
`being worked on <https://issues.apache.org/jira/browse/KAFKA-49>`_.

Multi-Produce
*************

Fetch
*****
Reading messages from a specific topic/partition combination.

Request to send to the broker::

     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /                           REQUEST HEADER                      /
    /                                                               /
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                               OFFSET                          |
    |                                                               |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                              MAX_SIZE                         |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    REQUEST_HEADER = See REQUEST_HEADER above
    OFFSET   = int(8) // Offset in topic and partition to start from. 64-bit.
    MAX_SIZE = int(4) // MAX_SIZE of the message set to return

Response::

     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /                          RESPONSE HEADER                      /
    /                                                               /
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /                        MESSAGES (0 or more)                   /
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Edge case behavior:

* If you request an offset that does not exist for that topic/partition 
  combination, you will get an OffsetOutOfRange error. While Kafka keeps 
  messages persistent on disk, it also deletes old log files to save space.
* FIXME: VERIFY -- If you request a fetch from a partition that does not exist,
  you will get a WrongPartition error.
* FIXME: VERIFY -- If the MAX_SIZE you specify is smaller than the largest
  message that would be fetched, you will get an InvalidFetchSize error.
* FIXME: VERIFY -- If you ask for an offset that is not at the start of a 
  message, you will receive 0 messages, but no error. This is a broken state
  that you should watch out for. Our approach when doing repeated fetches with
  brod is to do a check using the OFFSETS request if the first FETCH returns no
  messages.
* FIXME -- Try invalid topic, invalid partition reading
* FIXME -- Look at InvalidMessageSizeException

Normal, but possibly unexpected behavior:

* If you ask the broker for up to 300K worth  of messages from a given topic and
  partition, it will send you the appropriate headers followed by a 300K chunk
  worth of the message log. If 300K ends in the middle of a message, you get 
  half a message at the end. If it ends halfway through a message header, you 
  get a broken header. This is not an error, so be prepared to deal with the 
  situation.
* Kafka stores its messages in log files of a configurable size (512MB by
  default) called segments. A fetch of messages will not cross the segment 
  boundary to read from multiple files. So if you ask for a fetch of 300K's 
  worth of messages and the offset you give is such that there's only one 
  message at the end of that segment file, then you will get just
  one message back. The next time you call fetch with the following offset, 
  you'll get a full set of messages from the next segment file. Basically, 
  don't make any assumptions about how many messages are remaining from how 
  many you got in the last fetch.


Multi-Fetch
***********

Offsets
*******
FIXME: Put what it actually does here just as soon as we can understand it.

This one can be really deceptive. It is *not* a way to get the 
`offset that occurred at a specific time <https://issues.apache.org/jira/browse/KAFKA-87>`_.
Given special values for time, it can return you the earliest (-2) or latest 
(-1) offset for a topic/partition. Given any other time, it returns the offset
of the last (FIXME: VERIFY what he's saying)

Some background on how Kafka stores data:

Say your Kafka broker is configured to store its log files in /tmp/kafka-logs 
and you have a topic named "dogs", with two partitions. Kafka will create a 
directory for each partition::

    /tmp/kafka-logs/dogs-0
    /tmp/kafka-logs/dogs-1

Inside each of these partition directories, it will store the log for that 
topic+parition as a series of segment files. So for instance, in dogs-0, you 
might have::

    00000000000000000000.kafka
    00000000000536890406.kafka
    00000000001073761356.kafka

Each file is named after the offset represented by the first message in that 
file. The size of the segments are configurable (512MB by default). Kafka will 
write to the current segment file until it goes over that size, and then will
write the next message in new segment file. The files are actually slightly 
larger than the limit, because Kafka will finish writing the message -- a 
single message is never split across multiple files.


Jun's comments:
GetOffsetShell doesn't return the offset of every message. It returns the
offset of the first message in every segment file. If you provide a time,
the offsets you get back are based on the last modified time of the segment
files. There is a jira kafka-87 that tries to improve that.

Querying by timestamp might not work at all. See 
`Carlo's post <http://mail-archives.apache.org/mod_mbox/incubator-kafka-users/201108.mbox/%3CCAFbh0Q3QgsYXn4kjTTh0zZ9DJZ7J14tM5g24RFJSihFfbqDNyw@mail.gmail.com%3E>`_




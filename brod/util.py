import imp
import time
import sys
from optparse import OptionParser

import brod

def broderate():
	parser = OptionParser()
	parser.add_option("-H", "--host", dest="host", default="localhost", help="Kafka host")
	parser.add_option("-p", "--port", dest="port", type="int", default=9092, help="Kafka port")
	parser.add_option("-t", "--topic", dest="topic", help="Kafka topic")
	parser.add_option("-n", "--partition", dest="partition", type="int", default=0, help="Kafka topic partition")
	parser.add_option("-s", "--start", dest="start", type="int", default=brod.EARLIEST_OFFSET, help="Start offset")
	parser.add_option("-e", "--end", dest="end", type="int", default=brod.LATEST_OFFSET, help="End offset")

	(options, args) = parser.parse_args()
	module, funcname = args[0].split(":")
	host = options.host
	port = options.port
	topic = options.topic
	partition_num = options.partition
	start = options.start
	end = options.end

	kafka = brod.Kafka(host, port)
	if start in [brod.EARLIEST_OFFSET, brod.LATEST_OFFSET]:
		start = kafka.offsets(topic, start, 1)[-1]
	if end in [brod.EARLIEST_OFFSET, brod.LATEST_OFFSET]:
		end = kafka.offsets(topic, end, 1)[-1]
	
	partition = kafka.partition(topic, partition_num)
	total_megabytes = float(end - start) / 131072.
	print "Applying {0}:{1} to offsets {2}-{3} ({4} MB) from {5}-{6}".format(module, funcname, start, end, round(total_megabytes, 2), topic, partition_num)

	iterator = imp.load_source('iterator', module)
	func = getattr(iterator, funcname) 

	for status, messages in partition.poll(start, end):
		if messages:
			func(status, messages)
		else:
			break

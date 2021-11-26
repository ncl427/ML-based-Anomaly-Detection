from __future__ import print_function
import sys

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from elasticsearch import Elasticsearch
import time



			
def addId(data):
    j=json.dumps(data).encode('ascii', 'ignore')
    data['date'] = round(time.time() * 1000)
    return (data['date'], json.dumps(data))

def hadoop_Es(rdd):
	es_write_conf = {
		# specify the node that we are sending data to (this should be the master)
		"es.nodes" : 'localhost',
		# specify the port in case it is not the default port
		"es.port" : '9200',
		# specify a resource in the form 'index/doc-type'
		"es.resource" : 'sflowtest',
		# is the input JSON?
		"es.input.json" : "yes",
		# is there a field in the mapping that should be used to specify the ES document ID
		#"es.mapping.id": round(time.time() * 1000),
		'es.net.http.auth.user' : 'elastic',
		'es.net.http.auth.pass' : 'elastic'
	}
	#rddjson = rdd.map(lambda x: (round(time.time() * 1000), json.dumps(x)))
	rddjson = rdd.map(lambda x: addId(x))
	rddjson.saveAsNewAPIHadoopFile(path='-', outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=es_write_conf)

def hadoop_EsDest(rdd):
	es_write_conf = {
		# specify the node that we are sending data to (this should be the master)
		"es.nodes" : 'localhost',
		# specify the port in case it is not the default port
		"es.port" : '9200',
		# specify a resource in the form 'index/doc-type'
		"es.resource" : 'sflowtestdest',
		# is the input JSON?
		"es.input.json" : "yes",
		# is there a field in the mapping that should be used to specify the ES document ID
		#"es.mapping.id": round(time.time() * 1000),
		'es.net.http.auth.user' : 'elastic',
		'es.net.http.auth.pass' : 'elastic'
	}
	#rddjson = rdd.map(lambda x: (round(time.time() * 1000), json.dumps(x)))
	rddjson = rdd.map(lambda x: addId(x))
	rddjson.saveAsNewAPIHadoopFile(path='-', outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=es_write_conf)
			

if __name__ == "__main__":
	# SparkContext represents connection to a Spark cluster.
	conf = SparkConf()
	conf.setAppName("DataProcessing")
	conf.setMaster('local[6]')
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	
	# StreamingContext represents connection to a Spark cluster from existing SparkContext.
	ssc = StreamingContext(sc, 60)  # the number indicates how many seconds each batch lasts.
	
	# Creates an input stream that pulls events from Kafka.
	# Change IP and ports as needed
	kvs = KafkaUtils.createStream(ssc, "117.17.102.174:2181", "spark-streaming-consumer", {"flows":1})
	parsed = kvs.map(lambda x: json.loads(x[1]))
	
	# Get only elements that are needed and rename to make it clear.
	sflow_dict = parsed.map(lambda x: ({'srcAddr': x['SrcAddr'], 'srcPort': x['SrcPort'], 'dstAddr': x['DstAddr'], 'dstPort': x['DstPort'], 'sampledFlow': x['SequenceNum'], 'tcpFlags': x['TCPFlags'], 'protocol': x['Proto'], 'timestampStart': x['TimeFlowStart'], 'timestampEnd': x['TimeFlowEnd'], 'numBytes': x['Bytes'], 'numPackets': x['Packets'], 'vlanID': x['VlanId']}))
	#sflow_dict.pprint()
	
	# Get Sum of Flows sent from Source IP in window.
	sumOfFlows = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["sampledFlow"])).countByValue().map(lambda e: e[0][0]).countByValue()

	# Find VLANID for an specific srcAddress.	
	vlanIDs = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["vlanID"])).countByValue().map(lambda e: e[0])
	
	# Get sum of Bytes sent from Source IP in window.
	sumOfBytes = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["numBytes"])).reduceByKey(lambda x, y: x + y)
    
    # Get sum of Packets sent from Source Ip in Window.
	sumOfPackets = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["numPackets"])).reduceByKey(lambda x, y: x + y)
	
	# Get Sum of Flows sent from Destination IP in window.
	destSumOfFlows = sflow_dict.filter(lambda e: "dstAddr" in e).map(lambda e: (e["dstAddr"], e["sampledFlow"])).countByValue().map(lambda e: e[0][0]).countByValue()

	
	# Get sum of Bytes sent from Destination IP in window.
	destSumOfBytes = sflow_dict.filter(lambda e: "dstAddr" in e).map(lambda e: (e["dstAddr"], e["numBytes"])).reduceByKey(lambda x, y: x + y)
    
    # Get sum of Packets sent from Destination Ip in Window.
	destSumOfPackets = sflow_dict.filter(lambda e: "dstAddr" in e).map(lambda e: (e["dstAddr"], e["numPackets"])).reduceByKey(lambda x, y: x + y)
        
	
        
	
	# Count of unique dest IP that source IP talked to in window.
	# First map gets unique src/dst pairs.  Second map reduces just to srcAddr and counts number of uniq dstAddr.
	uniqDstIPs = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["dstAddr"])).countByValue().map(lambda e: e[0][0]).countByValue()
	
	# Count of unique source IP that dest IP talked to in window.
	# First map gets unique src/dst pairs.  Second map reduces just to dstAddr and counts number of uniq srcAddr.
	uniqSrcIPs = sflow_dict.filter(lambda e: "dstAddr" in e).map(lambda e: (e["dstAddr"], e["srcAddr"])).countByValue().map(lambda e: e[0][0]).countByValue()
	
	# Count of unique destination ports that source IP talked to in window.
	# First map gets unique src/dst pairs.  Second map reduces just to srcAddr and counts number of uniq dstPort.
	uniqDstPorts = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["dstPort"])).countByValue().map(lambda e: e[0][0]).countByValue()
	
	# Count of unique source ports that destination IP talked to in window.
	# First map gets unique src/dst pairs.  Second map reduces just to dstAddr and counts number of uniq srcPort.
	uniqSrcPorts = sflow_dict.filter(lambda e: "dstAddr" in e).map(lambda e: (e["dstAddr"], e["srcPort"])).countByValue().map(lambda e: e[0][0]).countByValue()
	
	
	join5 = sumOfFlows.join(vlanIDs).join(sumOfBytes).join(sumOfPackets).join(uniqDstIPs).join(uniqDstPorts)
	#join5.pprint()
	
	#Join destination data
	destjoin1 = destSumOfFlows.join(destSumOfBytes)
	destjoin2 = destjoin1.join(destSumOfPackets)
	destjoin3 = destjoin2.join(uniqSrcIPs)
	destjoin4 = destjoin3.join(uniqSrcPorts)
	

	
	# Map into format: (vlanID, SrcAddr, sumOfFlows, sumOfBytes, uniqDstIPs, uniqDstPorts).
	joined = join5.map(lambda e: ({"vlanID": e[1][0][0][0][0][1], "srcAddr": e[0], "sumOfFlows": e[1][0][0][0][0][0], "sumOfBytes": e[1][0][0][0][1], "uniqDstIPs": e[1][0][1], "uniqDstPorts": e[1][1], "sumOfPackets": e[1][0][0][1]}))
	joined.pprint(12)  # Show for all 12 IPs.
	
		
	# Map into format: (DstAddr, sumOfFlows, sumOfBytes, uniqDstIPs, uniqDstPorts).
	destJoined = destjoin4.map(lambda e: ({"dstAddr": e[0], "destSumOfFlows": e[1][0][0][0][0], "destSumOfBytes": e[1][0][0][0][1], "uniqSrcIPs": e[1][0][1], "uniqSrcPorts": e[1][1], "destSumOfPackets": e[1][0][0][1]}))
	destJoined.pprint(12)  # Show for all 12 IPs.
	
	
	# Send to ElasticSearch.

	joined.foreachRDD(hadoop_Es)
	destJoined.foreachRDD(hadoop_EsDest)


	
	# Start the execution of streams.
	ssc.start()
	
	# Wait for execution to stop.
	ssc.awaitTermination()

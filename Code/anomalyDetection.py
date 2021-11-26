
from __future__ import print_function


from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.stat import Statistics
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import StandardScalerModel
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, ArrayType, MapType, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id, PandasUDFType, col
from pyspark.sql.functions import stddev, stddev_pop, mean, when, explode, collect_list


import numpy as np
import pandas as pd
import joblib

from elasticsearch import Elasticsearch
import elasticsearch.helpers

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from math import sqrt, ceil
from itertools import cycle


import json
import sys
import requests
import time
import pyarrow



DEBUGMODE = True

# Initialize global variables.
kmeansModel = None
knnModel = None
standardScaler = None
schema = StructType([
	StructField('id', LongType()),
	StructField('sumOfBytes', DoubleType()),
	StructField('sumOfFlows', DoubleType()),
	StructField('sumOfPackets', DoubleType()),
	StructField('uniqDstIPs', DoubleType()),
	StructField('uniqDstPorts', DoubleType()),
	StructField('Prediction', LongType())
])

schema2 = StructType([
	StructField('id', LongType()),
	StructField('Prediction', LongType()),
	StructField('Probability', DoubleType())
])

schema3 = StructType([
	StructField('id', LongType()),
	StructField('Class', LongType()),
	StructField('Prob', DoubleType())
				])
	
FEATURE_COLS = ['sumOfBytes', 'sumOfFlows', 'sumOfPackets', 'uniqDstIPs', 'uniqDstPorts']
NORMAL = [0,  1, 5, 13, 8, 10, 11,  4, 12]
ANOMALOUS = [2, 3, 6, 7, 9]
a = []





def printDebugMsg(msg):
	if DEBUGMODE:
		print(msg)
		
def addId(data):
    j=json.dumps(data).encode('ascii', 'ignore')
    data['date'] = round(time.time() * 1000)
    return (data['date'], json.dumps(data))



def hadood_ES_Get():
	q ="""{"query": {"match_all": {}}}"""
	es_read_conf = {
		"es.nodes" : 'localhost',
		# specify the port in case it is not the default port
		"es.port" : '9200',
		# specify a resource in the form 'index/doc-type'
		"es.resource" : 'sflowtest',
        #Query
        "es.query" : q,
		# is the input JSON?
		"es.input.json" : "yes",
		#"mapred.reduce.tasks.speculative.execution": "false",
		#"mapred.map.tasks.speculative.execution": "false",
		# is there a field in the mapping that should be used to specify the ES document ID
		#"es.mapping.id": round(time.time() * 1000),
		'es.net.http.auth.user' : 'elastic',
		'es.net.http.auth.pass' : 'elastic'
	}
	#Query for elasticsearch data
	es_rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
								keyClass="org.apache.hadoop.io.NullWritable",
								valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
								conf=es_read_conf)
	df =sqlContext.createDataFrame(es_rdd)
	df.collect()
	return(df)

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
		#"mapred.reduce.tasks.speculative.execution": "false",
		#"mapred.map.tasks.speculative.execution": "false",
		# is there a field in the mapping that should be used to specify the ES document ID
		#"es.mapping.id": round(time.time() * 1000),
		'es.net.http.auth.user' : 'elastic',
		'es.net.http.auth.pass' : 'elastic'
	}
	#rddjson = rdd.map(lambda x: (round(time.time() * 1000), json.dumps(x)))
	#print(rdd.collect())
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
		#"mapred.reduce.tasks.speculative.execution": "false",
		#"mapred.map.tasks.speculative.execution": "false",
		# is there a field in the mapping that should be used to specify the ES document ID
		#"es.mapping.id": round(time.time() * 1000),
		'es.net.http.auth.user' : 'elastic',
		'es.net.http.auth.pass' : 'elastic'
	}
	#rddjson = rdd.map(lambda x: (round(time.time() * 1000), json.dumps(x)))
	rddjson = rdd.map(lambda x: addId(x))
	rddjson.saveAsNewAPIHadoopFile(path='-', outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=es_write_conf)

def hadoop_EsAno(rdd):
	es_write_conf = {
		# specify the node that we are sending data to (this should be the master)
		"es.nodes" : 'localhost',
		# specify the port in case it is not the default port
		"es.port" : '9200',
		# specify a resource in the form 'index/doc-type'
		"es.resource" : 'anomalies',
		# is the input JSON?
		"es.input.json" : "yes",
		#"mapred.reduce.tasks.speculative.execution": "false",
		#"mapred.map.tasks.speculative.execution": "false",
		# is there a field in the mapping that should be used to specify the ES document ID
		#"es.mapping.id": round(time.time() * 1000),
		'es.net.http.auth.user' : 'elastic',
		'es.net.http.auth.pass' : 'elastic'
	}
	#rddjson = rdd.map(lambda x: (round(time.time() * 1000), json.dumps(x)))
	rddjson = rdd.map(lambda x: addId(x))
	rddjson.saveAsNewAPIHadoopFile(path='-', outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=es_write_conf)
	

def checkAnomaly(data):
	# # # For each cluster:
	# # # 1) scale data.
	# # # 2) combine data into tuple.

	
	if not data.isEmpty():
		
		df = data.toDF()
		#removes empty data from RDD
		df= df.filter(df.srcAddr!='')
		#df.printSchema()
	#df.show(truncate=False)
	
	# Add unique numeric ID, and place in first column.
		df = df.withColumn("id", monotonically_increasing_id())
		print("DATA FORMATING INTO DATAFRAME")

		#df2 = df.select("id", FEATURE_COLS[0], FEATURE_COLS[1],FEATURE_COLS[2], FEATURE_COLS[3], FEATURE_COLS[4])
		
		#Convert all features to float
		df2 = df.select('id', *[col(c).cast('float').alias(c) for c in FEATURE_COLS])
	
		df.show()
		#df2.show()
		vecAssembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
		df_vector = vecAssembler.transform(df2).select('id','features') 
	# Drop other columns.
    # Scale the data.
    #We use the Scaler model trained on the original data
		df_scaledVector = standardScaler.transform(df_vector)
		#df_scaledVector.show()
		
		if not df_scaledVector.first() == None:
			scaleddf = df_scaledVector.rdd.map(extractVector).toDF(["id", "scaledFeatures", 'sumOfBytes', 'sumOfFlows', 'sumOfPackets', 'uniqDstIPs', 'uniqDstPorts'])
			scaleddfdrop = scaleddf.drop("scaledFeatures")
	#scaleddfdrop.show()
	
	#calls the define Pandas UDF for applying the sklearn KNN model to real-time data
	#Return Class Prediction and Prediction Probability
			df_map = scaleddfdrop.groupby("id").apply(predict_class_pandas_udf)
			#df_map.show()
			df_map = df_map.filter((df_map.Prob > 0.0) )
			df_prediction = df_map.join(df.drop(FEATURE_COLS[0], FEATURE_COLS[1],FEATURE_COLS[2], FEATURE_COLS[3], FEATURE_COLS[4]), 'id')
			#df_prediction = df_prediction.select("id","srcAddr", "vlanID", explode('Things').alias('Class', 'Prob'))
			
			#df_anomalous = df_prediction.filter((df_prediction.Prediction.isin(ANOMALOUS)) | (df_prediction.Probability < 0.99) ) 
			df_anomalous = df_prediction.filter((df_prediction.Class.isin(ANOMALOUS)))
			
			#df_prediction.printSchema()
			
			print("PREDICTION RESULTS")
			df_prediction.show()
	
			print("ANOMALOUS TRAFFIC")
			df_anomalous.show()
			anoDF = df_anomalous.drop('id', 'Class', 'Prob')
			anoDF = anoDF.dropDuplicates()
			anoRDD= anoDF.rdd.map(lambda e: ({"srcAddr": e[0], "vlanID": e[1]}))
			if not anoRDD.isEmpty():
				hadoop_EsAno(anoRDD)
				sendToIBN(anoDF)
			#print(anoRDD.collect())
		

	
@F.udf(returnType=DoubleType())
def predict_udf(*cols):
	return float(knnModel.predict_proba((cols,))[0,1])

@F.pandas_udf(returnType=ArrayType(DoubleType()))
def predict_pandas_udf(*cols):
    X = pd.concat(cols, axis=1)
    return pd.Series(row.tolist() for row in knnModel.predict_proba(X))


#Pandas UDF for applying the sklearn KNN model to real-time data
@F.pandas_udf(schema3, PandasUDFType.GROUPED_MAP)
def predict_class_pandas_udf(cols):
	#ids = cols['id']
	xNormal = cols[FEATURE_COLS]
	#pred = knnModel.predict(xNormal)  #not needed as we are going to use the prediction probability result
	predProb = knnModel.predict_proba(xNormal)  #returns numpy array of class probabilities
	#labels = np.argmax(predProb, axis=1)  #Keeps the columns with the max value of Prob (Class labels that appear in the prediction)
	classes = knnModel.classes_ #Full class label list
	#pred = [classes[i] for i in labels] #Label the classes based on the index of the array
	probClasses = pd.DataFrame(predProb, columns = classes) #Dataframe of class predictions
	probClasses = probClasses.loc[:, (probClasses != 0).any(axis=0)]  #Drops the columns with 0 values, to keep only relevant classes
	#l = probClasses.to_dict('records')
	i = probClasses.to_dict('split')
	x_testReset = cols.reset_index()
	x_values = pd.DataFrame({"Prob" : i['data']})
	#y_values = pd.DataFrame({"Class" : l})   Fix in python 3.7
	#x_values = x_values.explode('Prob')
	x_testReset['Prob'] = x_values['Prob']
	#x_testReset = x_testReset.explode('Class')   Fix in Python 3.7
	x_testReset = x_testReset.explode('Prob')
	x_testReset = x_testReset.reset_index()
	##Get the right classes
	a=i['columns']  #For the iterator of classes
	i = iter(a)
	x_testReset['Class'] = x_testReset.index.map(dict(zip(x_testReset.index, cycle(i))))
	#x_testReset['Class'] = pd.to_numeric(x_testReset['Class'])
	#x_testReset["Class"] = y_values['Class']

	
#	maxProb = np.amax(predProb, axis=1)
#	predpdNormal = pd.DataFrame(pred, columns = ['Prediction'])
#	predProb = pd.DataFrame(maxProb, columns = ['Probability']).reset_index()
#	predpdNormalReset = predpdNormal.reset_index()
#	x_testReset = cols.reset_index()
#	x_testReset["Prediction"] = predpdNormalReset["Prediction"]
#	x_testReset["Probability"] = predProb["Probability"]

	

	#x_testReset['id'] = pred
	return x_testReset[['id', 'Class', 'Prob']]
	#return x_testReset[['id', 'Prediction', 'Probability']]
	#return pd.DataFrame({'id': pred, 'Prediction': ids})
		

def sendToIBN(anoDF):
	# First get a list of all hosts.
	# Create dictionary mapping from IP to host switch.
	
	print("Send Mitigation Action to IBN tool")
	# Configure parameters needed for POST request
	anomalous = anoDF.toJSON().map(lambda j: json.loads(j)).collect()
	print(anomalous)
	urlToPost = "http://192.168.1.11:5000/api/mitigate/"
	print("action = {0}".format(urlToPost))
	#resp = requests.post(urlToPost, data=anomalous)
	#print("returns {0}".format(resp))
				
		
def extractVector(row):
	# From https://stackoverflow.com/questions/38384347/how-to-split-vector-into-columns-using-pyspark
	return (row["id"],row["scaledFeatures"]) + tuple(row['scaledFeatures'].toArray().tolist())

def extract(row):
	# From https://stackoverflow.com/questions/38384347/how-to-split-vector-into-columns-using-pyspark
	return (row["id"], row["prediction"], row["scaledFeatures"]) + tuple(row['scaledFeatures'].toArray().tolist())


#Not used, please use the Jupyter notebook instead. (pre-training)
def trainML():
	
	
	df = hadood_ES_Get()
	#Fortmat the Dataframe into proper Feature columns
	df=df.rdd.map(lambda x: \
				  (x._2["sumOfBytes"],x._2["sumOfFlows"], x._2["sumOfPackets"],x._2["uniqDstIPs"], x._2["uniqDstPorts"] )) \
	.toDF(['sumOfBytes', 'sumOfFlows', 'sumOfPackets', 'uniqDstIPs', 'uniqDstPorts'])
	df.printSchema()
	df.show()
	
	# Add unique numeric ID, and place in first column.
	df = df.withColumn("id", monotonically_increasing_id())
	df = df.select("id", FEATURE_COLS[0], FEATURE_COLS[1], FEATURE_COLS[2],FEATURE_COLS[3],FEATURE_COLS[4])
	df.show()
	

	# Convert all data columns to float.
	df = df.select('id', *[col(c).cast('float').alias(c) for c in FEATURE_COLS])
	df.show()
	
	#Saves the dataset as csv
	df.repartition(1).write.csv("cc_out.csv")
	
	# Need to convert this to a vector for Spark's implementation of KMeans.
	vecAssembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
	df_kmeans = vecAssembler.transform(df).select('id', 'features')  # Drop other columns.
	df_kmeans.show()
	
	
	#Scale the data by using StandardScaler on a Vector of Features.
	#WE NEED TO SAVE THIS TRAINED MODEL FOR FUTURE TEST DATA
	
	# Scale the data.
	scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
	scaler_model = scaler.fit(df_kmeans) #Remeber this model and use it for new data
	df_scaled = scaler_model.transform(df_kmeans)
	df_scaled.show()
	
	#Saves the Scaler model to file
	scaler_model.save('scaler5.sav')
	
	#TRAIN KMEANS WITH VALUE OF K
	k = 9
	kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("scaledFeatures")
	model = kmeans.fit(df_scaled)
	centers = model.clusterCenters()
	print("Cluster Centers: ")
	for center in centers:
		print(center)
	# Assign events to clusters. Testing the model
	predictions = model.transform(df_scaled).select('id', 'scaledFeatures', 'prediction')
	predictions.show()
	# Extract scaledFeatures column back to FEATURE_COLS
	predictions = predictions.rdd.map(extract).toDF(["id", "prediction", "scaledFeatures", 'sumOfBytes', 'sumOfFlows', 'sumOfPackets', 'uniqDstIPs', 'uniqDstPorts'])
	# Rename scaledFeatures to features.
	predictions = predictions.withColumnRenamed("scaledFeatures", "features")
	df_pred = predictions
	df_pred.show()
	df_pred_plot = df_pred.drop('features')
	df_pred_plot.show()
	print("Prediction counts for each cluster:")
	predictions.groupBy('prediction').count().show()
	
	# Evaluate clustering by computing Silhouette score
	evaluator = ClusteringEvaluator()
	silhouette = evaluator.evaluate(predictions)
	printDebugMsg("Silhouette with squared euclidean distance = {0}".format(silhouette))
	

	# Update global variables.
	global kmeansModel

	kmeansModel = model
	
	
	
if __name__ == "__main__":
	# SparkContext represents connection to a Spark cluster.
	print("KOREN ANOMALY DETECTION V1")
	conf = SparkConf()
	conf.setAppName("AnomalyDetection")
	conf.setMaster('local[6]')
	sc = SparkContext(conf=conf)
	
	#initialize SQLContext from spark cluster
	sqlContext = SQLContext(sc)
	sc.setLogLevel("WARN")
	
	spark = SparkSession \
	.builder \
	.config(conf=conf) \
	.getOrCreate()
	
	#trainML()
	
	# StreamingContext represents connection to a Spark cluster from existing SparkContext.
	ssc = StreamingContext(sc, 60)  # the number indicates how many seconds each batch lasts.
	
	# Creates an input stream that pulls events from Kafka.
	
	kvs = KafkaUtils.createStream(ssc, "220.149.42.169:2181", "spark-streaming-consumer", {"flows":1})
	#kvs = KafkaUtils.createDirectStream(ssc, topics=["flows"], kafkaParams={"bootstrap.servers":"220.149.42.169:9092"})
	parsed = kvs.map(lambda x: json.loads(x[1]))
	
	# Get only elements that are needed and rename to make it clear.
	sflow_dict = parsed.map(lambda x: ({'srcAddr': x['SrcAddr'], 'srcPort': x['SrcPort'], 'dstAddr': x['DstAddr'], 'dstPort': x['DstPort'], 'sampledFlow': x['SequenceNum'], 'tcpFlags': x['TCPFlags'], 'protocol': x['Proto'], 'timestampStart': x['TimeFlowStart'], 'timestampEnd': x['TimeFlowEnd'], 'numBytes': x['Bytes'], 'numPackets': x['Packets'], 'vlanID': x['VlanId']}))
	#sflow_dict.pprint()
	
	# Get Sum of Flows sent from Source IP in window.
	#sumOfFlows = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["numBytes"])).reduceByKey(lambda x, y: x + y)
	sumOfFlows = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["sampledFlow"])).countByValue().map(lambda e: e[0][0]).countByValue()

	# Find VLANID for an specific srcAddress.	
	vlanIDs = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["vlanID"])).countByValue().map(lambda e: e[0])
	
	# Get sum of Bytes sent from Source IP in window.
	sumOfBytes = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["numBytes"])).reduceByKey(lambda x, y: x + y)
    
    # Get sum of Packets sent from Source Ip in Window.
	sumOfPackets = sflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["numPackets"])).reduceByKey(lambda x, y: x + y)
	
	# Get Sum of Flows sent from Destination IP in window.
	#destSumOfFlows = sflow_dict.filter(lambda e: "dstAddr" in e).map(lambda e: (e["dstAddr"], e["numBytes"])).reduceByKey(lambda x, y: x + y)
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
	#destJoined.pprint(12)  # Show for all 12 IPs.
	
	
	
	# Check for anomaly.
	#Load Previously trained models
	#KNN -sklearn
	#StandardScaler- spark

	knnModel = joblib.load('python/knn.sav')
	standardScaler = StandardScalerModel.load('python/scaler.sav')
	joined.foreachRDD(hadoop_Es)
	destJoined.foreachRDD(hadoop_EsDest)
	joined.foreachRDD(checkAnomaly)
	
	# Start the execution of streams.
	ssc.start()
	
	# Wait for execution to stop.
	ssc.awaitTermination()

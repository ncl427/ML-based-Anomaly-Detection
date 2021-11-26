# ML-based-Anomaly-Detection
Anomaly detection system using ML algorithms for clustering unlabeled traffic data, and apply classification based on the clusters. The implementation follows a Data Stream approach where open source utilities like Apache Kafka, Apache Spark and Elasticsearch are utilized. 

![Anomaly-Detection-System](figures/completeSystem.png)

## Description
The proposed system aims to provide a real-time data stream operation and big data processing framework to achieve immediate and resilient network anomaly detection. By also providing mitigation actions from high level entities like the **IBN tool** which is able to manage the underlying network by means of API flow insertion rules. The status of the system can be observed thanks to a **real-time monitoring** implementation that collects information from hosts and switches.

The overall system is composed of the use of s**Flow configuration** on data plane switches that periodically send traffic data to a data streams pipeline connected to and **sFlow Collector** and **Apache Kafka**. This high volume of data is provisioned to **Apache Spark** for real-time big data processing and anomaly detection. Also, the RAW, processed and Anomalous data are sent to an ELK (Elasticsearch, Logstash, Kibana) stack for historical records. 

Unsupervised learning techniques (**K-Means**) and supervised learning classifier (**KNN**) are used for the anomaly detection.<br>
**Prometheus** and **Grafana** are used for the real time monitoring system that receives the data from the hosts and switches inside the network. <br>
The **IBN tool** receives the mitigation actions from the Anomaly Detection system and applies the flow rules for deletion of anomalous traffic on the basis of vlanIDs<br>
The whole system aims to guarantee the operation of the anomaly detection mechanism when the volume of data exponentially increases due to flood network attacks.

## Deployment of the system

The system has been deployed in 4 server machines (Make sure they can communicate with each other), but it can be deployed in a single machine if the computing resources are sufficient. Be mindful that if deploying in a single machine the use of deployment environments (such as anaconda) are adviced as there are specific version requirements dependent on the version of python that is used. 

Hardware                                                                                                              | Software       | Libraries
----------------------------------------------------------------------------------------------------------------------| -------------  | -------------
**Server 1 - SmartStack** <br> Ubuntu 18.04.5 with 250 GB of RAM and <br>64 logical CPU cores, 4 Tera drive               | elasticsearch-7.15.0 <br> logstash-7.15.0 <br> kibana-7.15.0 <br> spark-2.3.0-hadoop2.7 <br> Java 8   | python 3.6<br>pyarrow 0.15 <br> scikit-learn 0.23.0 <br> joblib 1.1.0 <br> pandas 1.1.5 <br> pyspark 3.2.0
**Server 2 - Flow Stream Collector** <br> Ubuntu 18.04.5 with 64 GB of RAM and <br>40 logical CPU cores, 2 Tera drive     | goflow2 v1.0.4<br>kafka_2.12-2.8.1 | python 3.8.8
**Server 3 - Monitoring System** <br> Ubuntu 18.04.5 with 16 GB of RAM and <br>16 logical CPU cores, 2 Tera drive         | Grafana 8.2 <br> Prometheus 3.2 <br> Prometheus Gateway | python 3.0 <br> pandas 1.3.0 <br> numpy 1.20.3 <br> request 2.26.0
**Server 4 - IBN Tool** <br> Ubuntu 18.04.5 with 32 GB of RAM and <br>16 logical CPU cores, 1 Tera drive                  | Mariadb 10.3 <br> Apache2 2.2.41 <br> phpmyadmin 5.0.0-1    | Python 3.8.5 <br> flask 1.1.2

### Network Infrastructure
This particular implementation uses the KOREN infrastructure -> "KOREN (Korea Advanced Research Network) is a non-profit testbed network infrastructure established for facilitating research and development and international joint research cooperation. It provides quality broadband network testbed for domestic and international research activities to the industry, academia, and research institutions, enabling testing of future network technologies and supporting R&D on advanced applications"

<https://www.koren.kr/eng/Intro/intro01.asp>

The swtiches have been configured with sFlow agents to export flow information.

Any other network capable of sending sFlow data can be used with our project. Even a virtual network made in mininet. The important configuration that needs to be considered is that the Sampling rate for the sflow packets needs to be set at **1024** if the ML are trained with the provided dataset in this repository. As the data was collected with that rate, and to avoid issues with the classification.

If collecting your own data, this setting can be configured as pleased. Just be mindful to not modify it at later steps as it may cause issues with the behaviour of the system (Do not change the value once you have collected processed data)

### Flow Stream Collector
Contains the implementation of the opensource sFlow collector **goflow2**  <https://github.com/netsampler/goflow2>
this collector is used to get sFlow packets from the configured switches. There are binary files available for download.

Also **apache kafka** is deployed in this system

Once kafka is running in the system it is necessary to create a topic for keeping the sflow traffic send by the collector.

`bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic flows`

You can replace localhost with an specific ip of your server IF needed. Do not change the port.

Then, we start the sflow collector with this command

`goflow2 -transport=kafka -transport.kafka.brokers=localhost:9092 -transport.kafka.topic=flows`

We use JSON formating in our Spark application so, DO NOT activate the option of using protobuf. Allso, You can replace localhost with an specific ip of your server IF needed.

### SmartStack
Be sure to download and deploy elasticsearch, logstash, kibana and spark as per the system requirements in the table. Please be mindful of the versions. Specially pyarrow and pandas. It is recommended to create a CONDA environment with python version 3.6 and use it for running the Spark commands and the Jupyter notebooks from this project.

Confgiruation files have been provided in the **Config/** folder, the **kafka.conf** file should be placed inside the logstash config folder, and the IP needs to be changed to reflect the IP of the Kafka server and elasticsearch (Bi mindful of the user name and password.)

If you enable security for ELK (which you should) try to use the same user name and password as provided in this project to avoid issues, as this is used in multiple parts of the code. (USER: elastic, PASSWORD: elastic)

Place the **spark-env.sh** and **spark-defaults.conf** inside the **conf/** folder of your Spark deployment. 

After this you can launch your deployment as this:

**for Spark, execute this commands inside the spark folder**

`sbin/start-master.sh`

`sbin/start-slave.sh spark://YOURIPADDRESS:7077`

Change to your ip or hostname.

**Then you can launch elasticsearch and kibana**.

Inside the elasticsearch folder
`bin/elasticsearch`

Inside the kibana folder
`bin/kibana`

Inside logstash folder
`bin/logstash -f config/kafka.conf`

Once Spark is running, place the files contained inside the **Code/** folder of this repository (anomalyDetection.py, dataProcessing.py) inside the **python** folder of the spark deployment.
Also, you need to download the **elasticsearch-spark-20_2.11-7.15.0.jar** from the <https://www.elastic.co/downloads/hadoop> site and place the jar inside the spark folder. 

**If you are collecting data without applying ML run**

`bin/spark-submit --jars elasticsearch-spark-20_2.11-7.15.0.jar --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 python/dataProcessing.py | grep -v INFO | grep -v WARN`

The spark application will collect data (If you already have traffic from your network) process it and send it to elasticsearch. The indexes will be created automatically. 

**If you have trained your models from** <https://github.com/ncl427/ML-based-Anomaly-Detection/tree/main/Code/TrainML>
--Follow the instructions over there and place the generated models inside the **python** directory of spark. --

`bin/spark-submit --jars elasticsearch-spark-20_2.11-7.15.0.jar --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 python/anomalyDetection.py | grep -v INFO | grep -v WARN`

Anomaly detection will start alongside data processing.

**IMPORTANT** Just run one of this at a time, the **anomalyDetection.py** implementation already has the processing capabilitites, so you do not need to run the other python code when your models are already trained.

**IMPORTANT** Do not forget to modify the code provided with the IP information of your servers- KAFKA and Elasticsearch. 

**IMPORTANT** Java 8 is needed, do not install other versions as it may break your whole operation.


### IBN Tool

Details of implementation can be seen in this repo

<https://github.com/TalhaJadoon/KOREN-Project>


### Real-time monitoring

Details of implementation can be seen in this repo
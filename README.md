# ML-based-Anomaly-Detection
Anomaly detection system using ML algorithms for clustering unlabeled traffic data, and apply classification based on the clusters. The implementation follows a Data Stream approach where open source utilities like Apache Kafka, Apache Spark and Elasticsearch are utilized. 

![Anomaly-Detection-System](figures/completeSystem.png)

The proposed system aims to provide a real-time data stream operation and big data processing framework to achieve immediate and resilient network anomaly detection. By also providing mitigation actions from high level entities like the **IBN tool** which is able to manage the underlying network by means of API flow insertion rules. The status of the system can be observed thanks to a **real-time monitoring** implementation that collects information from hosts and switches.

The overall system is composed of the use of s**Flow configuration** on data plane switches that periodically send traffic data to a data streams pipeline connected to and **sFlow Collector** and **Apache Kafka**. This high volume of data is provisioned to **Apache Spark** for real-time big data processing and anomaly detection. Also, the RAW, processed and Anomalous data are sent to an ELK (Elasticsearch, Logstash, Kibana) stack for historical records. 

Unsupervised learning techniques (**K-Means**) and supervised learning classifier (**KNN**) are used for the anomaly detection.
**Prometheus** and **Grafana** are used for the real time monitoring system that receives the data from the hosts and switches inside the network.
The **I**BN tool** receives the mitigation actions from the Anomaly Detection system and applies the flow rules for deletion of anomalous traffic on the basis of vlanIDs
The whole system aims to guarantee the operation of the anomaly detection mechanism when the volume of data exponentially increases due to flood network attacks.
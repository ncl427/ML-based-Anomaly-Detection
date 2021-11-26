# Code related to the Anomaly Detection System
## Contents
### Files
**dataProcessing.py**
Python code that runs in Apache Spark using the pyspark libraries. The functionality of this code is:
* Process sFlow Data streams in real time
* Inputs the processed data inside elasticsearch indexes

This code is used in order to start gathering data for creation of a dataset. 

**anomalyDetection.py**
Python code that runs in Apache Spark using the pyspark libraries. The functionality of this code is:
* Process sFlow Data streams in real time
* Applies KNN model (Loaded from the TrainML/Training.ipynb | TrainML/TrainingCSV.ipynb output)
* Inputs processed data inside elasticsearch indexes
* Inputs anomalous data (if any) inside elasticsearch indexes
* Sends mitigation call to the IBN Tool (Or any other Network Orchestrator/Manager) for flow insertion into the Network Infrastructure

This code is used once we have trained models of ML algorithms. Replaces the functionality of  **dataProcessing.py** (No need to use that anymore)

### Folders
**TrainML**

* Contains the Jupyter Notebooks used for training the ML models, and also performance comparisson and tuning.

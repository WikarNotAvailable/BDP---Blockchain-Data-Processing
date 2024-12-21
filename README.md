# BDP-Blockchain-Data-Processing
University group project, which main goals are to extract, store and process blockchain big data with cloud computing in order to detect anomalies using machine learning and statistic methods.

Run "python -m pip install -e ." to create local package that helps in managing imports

Creating datasets for local testing
 ETL - run main.py and main_benchmarks.py in etl folder

Aggregations - run aggregations.py in etl folder 
![image](https://github.com/user-attachments/assets/28b7da35-77e7-4ca7-82d5-90a6ef989e99)
these directories needs to be changes for benchmark data

Preprocessing (preprocessing folder in anomalies_detection folder) - 
a) For benchmark data run preprocess_benchmark_datasets.py (once)
b) For training data run preporcess_training_datasets.py (3 times, change step from 1-3 every time):

![image](https://github.com/user-attachments/assets/1eda202e-a3c9-4c7f-9f3f-047b5dddd79f)

## Data Processing of Yelp Top Store Text File

The purpose of this session is to run a pyspark script to process the yelp top store text file, and provision with the schema to contruct the tabular data 
and save the preprocessing results to SqLite db. 


## Usage
* Need `sqlite-jdbc-3.8.6.jar` driver file in order to connect to sqlite database
* Input data file `topStores.txt`  
Running command `spark-submit --driver-class-path ./jar/sqlite-jdbc-3.8.6.jar --jars ./jar/sqlite-jdbc-3.8.5.jar topstore_load.py ./sample_data/topStores.txt`


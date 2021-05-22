from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SQLContext,DataFrame,Row



#.set("spark.executor.extraClassPath","/Users/suyile/Documents/yelp/ETL/sqlite-jdbc-3.8.6.jar")\
#.set("spark.driver.extraClassPath","/Users/suyile/Documents/yelp/ETL/sqlite-jdbc-3.8.6.jar")\
#.set("spark.jars","/Users/suyile/Documents/yelp/ETL/sqlite-jdbc-3.8.6.jar")

#${SPARK_HOME}/bin/spark-submit --master=local --driver-class-path ./sqlite-jdbc-3.8.6.jar --jars ./sqlite-jdbc-3.8.5.jar topstore_load.py



storeSchema = StructType([
    StructField('city', StringType(), False),
    StructField('season',StringType(),False),
    StructField('store_id',StringType(),False),
    StructField('store_name',StringType(),False),
])

conf = SparkConf().setAppName('Euler')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
input=sys.argv[1]
#output=sys.argv[2]

def state_to_city(state):
    return {
        'EDH':'Edinburgh' ,
        'BW':'Karlsruhe',
        'QC':'Montreal',
        'ON':'Waterloo',
         'PA':'Pittsburgh',
         'NC':'Charlotte',
        'IL':'Urbana-Champaign',
         'AZ':'Phoenix',
         'NV': 'Las Vegas',
         'WI':'Madison',
    }[state]

store=Row('city','season','store_id','store_name')

def line_to_object(state,season,stores):
    store_list=stores.strip().split('|')
    res=[]
    city = state_to_city(state)
    for i in xrange(0, len(store_list)):
        store=store_list[i]
        s = store.strip().split(',')
        store_id=s[0]
        store_name=s[1].encode('ascii', 'ignore')
        store=(city,season,store_id,store_name)
        res.append(store)

    return res

res=sc.textFile(input).map(lambda line : line.strip().split('::')).\
    map(lambda line : (line[0],line[1],line[2].encode('ascii', 'ignore')))\
    .flatMap(lambda (state, season, stores):line_to_object(state, season, stores))

url="jdbc:sqlite:/Users/suyile/Documents/yelp/web/db.sqlite3 "
table="recommend_stores"

df = sqlContext.createDataFrame(res, storeSchema)
df.write.jdbc(url, table, 'overwrite')
df.show()

df.saveAsTextFile(output)
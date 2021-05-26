from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SQLContext,DataFrame, Row

"""
Another way to set up jar for spark conf
Please check spark official dataframe: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
conf.set("spark.executor.extraClassPath", "./jar/sqlite-jdbc-3.8.6.jar")
conf.set("spark.driver.extraClassPath", "./jar/sqlite-jdbc-3.8.6.jar")
conf.set("spark.jars", "./jar/sqlite-jdbc-3.8.6.jar")
"""

# Define the Schema of Table
storeSchema = StructType([
    StructField('city', StringType(), False),
    StructField('season', StringType(), False),
    StructField('store_id', StringType(), False),
    StructField('store_name', StringType(), False),
])

# store = Row('city','season','store_id','store_name')

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



def line_to_object(state, season, stores):
    store_list = stores.strip().split('|')
    res = []
    city = state_to_city(state)
    for i in xrange(0, len(store_list)):
        store = store_list[i]
        s = store.strip().split(',')
        store_id = s[0]
        store_name = s[1].encode('ascii', 'ignore')
        store = (city, season, store_id, store_name)
        res.append(store)

    return res

def main():
    ## Taking the input txt file for processing
    input = sys.argv[1]

    conf = SparkConf().setAppName('data_processing')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    res = sc.textFile(input) \
        .map(lambda line: line.strip().split('::')) \
        .map(lambda line: (line[0], line[1], line[2].encode('ascii', 'ignore'))) \
        .flatMap(lambda (state, season, stores): line_to_object(state, season, stores))

    df = sqlContext.createDataFrame(res, storeSchema)

    # Connect to database and write the result to sqlite db
    url = "jdbc:sqlite:/Users/suyile/Documents/yelp/web/db.sqlite3 "
    table = "recommend_stores"
    df.write.jdbc(url, table, 'overwrite')

    # Save sample output to txt file
    # output=sys.argv[2]
    # df.saveAsTextFile(output)



if __name__=="__main__":
    main()
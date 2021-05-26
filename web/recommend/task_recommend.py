#!/usr/bin/env python

import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS
from spark_celery import SparkCeleryApp, SparkCeleryTask, RDD_builder, main

BROKER_URL = 'amqp://yiles:jacobrabbitmq@gateway.sfucloud.ca:5672/yilesvhost'
# BROKER_URL = 'amqp://guest:guest@localhost:5672//'
BACKEND_URL = 'rpc://'


def sparkconfig_builder():
    from pyspark import SparkConf
    return SparkConf().setAppName('SparkCeleryTask') \
        .set('spark.dynamicAllocation.enabled', 'true') \
        .set('spark.dynamicAllocation.schedulerBacklogTimeout', 1) \
        .set('spark.dynamicAllocation.minExecutors', 1) \
        .set('spark.dynamicAllocation.executorIdleTimeout', 20) \
        .set('spark.dynamicAllocation.cachedExecutorIdleTimeout', 60)


def parseRating(line):
    """
    Parses a rating record, format busId::userID::rating::date .
    """
    fields = line.strip().split("::")

    # hash, userID, busID, rating
    return long(fields[3][9]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))


def parseStore(line):
    """
    Parses a store record in MovieLens format movieId::movieTitle .
    """
    fields = line.strip().split("::")

    # busID, busName + busAddress
    return int(fields[0]), fields[1] + '::' + fields[4]


def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
        .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
        .values()

    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

def city_to_state(city):
    return {
        'Edinburgh': 'EDH',
        'Karlsruhe': 'BW',
        'Montreal': 'QC',
        'Waterloo': 'ON',
        'Pittsburgh': 'PA',
        'Charlotte': 'NC',
        'Urbana-Champaign': 'IL',
        'Phoenix': 'AZ',
        'Las+Vegas': 'NV',
        'Madison': 'WI',
    }[city]

app = SparkCeleryApp(broker=BROKER_URL, backend=BACKEND_URL, sparkconfig_builder=sparkconfig_builder)

# Setting priority for workers allows primary workers, with spillover if the primaries are busy. Used to minimize the
# number of Spark contexts (active on the cluster, or caching common data). Works only with Celery >= 4.0
# Run a lower-priority consumer like this:
#   CONSUMER_PRIORITY=5 spark-submit --master=yarn-client demo.py
import os
from kombu import Queue

priority = int(os.environ.get('CONSUMER_PRIORITY', '10'))
app.conf['CELERY_QUEUES'] = (
    Queue('celery', consumer_arguments={'x-priority': priority}),
)


class StoreRecommender(SparkCeleryTask):
    name = 'StoreRecommender'

    @RDD_builder
    def get_userdata(self, newRating):
        # build RDD for the new user ratings
        newRating = newRating.split('?')[1]
        newRatingList = newRating.split('&')
        season = newRatingList[0].split('=')[1]
        city = newRatingList[1].split('=')[1]

        newRatingList = newRatingList[2:]
        clean_up_rating_list = []
        for rating in newRatingList:
            rating = rating.split('=')
            rating = map(int, rating)
            clean_up_rating_list.append(rating)
        new_rating_rdd = app.sc.parallelize(clean_up_rating_list).cache()
        new_rating_rdd = new_rating_rdd.map(lambda (bus_id, rating): (-1, bus_id, rating))
        return new_rating_rdd.cache(), season, city

    def run(self, newRating):
        print('===========newRating=========')
        print(newRating)
        (myRatingsRDD, season, city) = self.get_userdata(newRating)
        myRatings = myRatingsRDD.collect()
        print("====================RDD=================")
        print(myRatingsRDD.collect())

        state = city_to_state(city)
        print("==========state==================")
        print(state)

        # recommendHomeDir = '/Users/suyile/Documents/yelp/recommend_data/'
        recommendHomeDir = '/user/jza201/recommend_data/'
        # ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
        ratings = app.sc.textFile(join(recommendHomeDir, "rating_del_new_id_" + season + "_" + state)).map(parseRating).cache()

        # stors is an RDD of (businessId, businessName)

        stores = app.sc.textFile(join(recommendHomeDir, "bus_del_new_id_address")).map(parseStore).distinct().cache()

        numRatings = ratings.count()
        numUsers = ratings.values().map(lambda r: r[0]).distinct().count()
        numStores = ratings.values().map(lambda r: r[1]).distinct().count()

        print "Got %d ratings from %d users on %d stores." % (numRatings, numUsers, numStores)

        numPartitions = 4
        training = ratings.values() \
            .union(myRatingsRDD) \
            .repartition(numPartitions) \
            .cache()


        rank = 12
        numIter = 20
        lmbda = 0.4
        model = ALS.train(training, rank, numIter, lmbda)

        # make personalized recommendations

        myRatedStoreIds = set([x[1] for x in myRatings])
        candidates = app.sc.parallelize([s for s in stores.keys().collect() if s not in myRatedStoreIds])
        predictions = model.predictAll(candidates.map(lambda x: (-1, x))).collect()
        recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:20]

        recommendations_id = [row[1] for row in recommendations]
        print("================recommendations_id=============")
        print(recommendations_id)
        ratings = [row[2] for row in recommendations]
        ratings_rdd = app.sc.parallelize(ratings, 1).cache()

        store_names = stores.filter(lambda (s_id, name): s_id in recommendations_id) \
            .map(lambda (s_id, name): (name)).repartition(1).cache()


        outdata = store_names.zip(ratings_rdd).map(lambda (name, score): u"%s::%.2f" % (name, score)).collect()

        return outdata


app.tasks.register(StoreRecommender)

if __name__ == "__main__":

    main()

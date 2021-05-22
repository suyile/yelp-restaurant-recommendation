#!/usr/bin/env python

from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS
from spark_celery import SparkCeleryApp, SparkCeleryTask, RDD_builder, main

BROKER_URL = 'amqp://yiles:jacobrabbitmq@gateway.sfucloud.ca:5672/yilesvhost'
BACKEND_URL = 'rpc://'


def sparkconfig_builder():
    # from pyspark import SparkConf
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

    # busID, busName
    return int(fields[0]), fields[1]


def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    print predictions.map(lambda x: ((x[0], x[1]), x[2])).take(20)
    print data.map(lambda x: ((x[0], x[1]), x[2])).take(20)
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
        .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
        .values()

    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))


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
        clean_up_rating_list = []
        for rating in newRatingList:
            rating = rating.split('=')
            rating = map(int, rating)
            clean_up_rating_list.append(rating)
        new_rating_rdd = app.sc.parallelize(clean_up_rating_list).cache()
        new_rating_rdd = new_rating_rdd.map(lambda (bus_id, rating): (-1, bus_id, rating))
        return new_rating_rdd.cache()

    def run(self, newRating, season):
        print('===========newRating=========')
        print(newRating)
        myRatingsRDD = self.get_userdata(newRating)
        myRatings = myRatingsRDD.collect()
        print(myRatingsRDD.collect())

        # recommendHomeDir = '/user/jza201/recommend_data/'
        recommendHomeDir = '/Users/suyile/Documents/yelp/recommend_data/'
        # ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
        ratings = app.sc.textFile(join(recommendHomeDir, "rating_del_new_id_"+season)).map(parseRating)

        # stors is an RDD of (businessId, businessName)
        stores = app.sc.textFile(join(recommendHomeDir, "bus_del_open_new_id")).map(parseStore).cache()

        numRatings = ratings.count()
        numUsers = ratings.values().map(lambda r: r[0]).distinct().count()
        numStores = ratings.values().map(lambda r: r[1]).distinct().count()

        print '**************'
        print ratings.values().take(10)
        print '**************'
        print myRatingsRDD.collect()
        print "Got %d ratings from %d users on %d stores." % (numRatings, numUsers, numStores)

        # split ratings into train (60%), validation (20%), and test (20%) based on the
        # last digit of the timestamp, add myRatings to train, and cache them

        # training, validation, test are all RDDs of (userId, movieId, rating)

        numPartitions = 4
        training = ratings.filter(lambda x: x[0] < 6) \
            .values() \
            .union(myRatingsRDD) \
            .repartition(numPartitions) \
            .cache()

        validation = ratings.filter(lambda x: x[0] >= 6 and x[0] < 8) \
            .values() \
            .repartition(numPartitions) \
            .cache()

        test = ratings.filter(lambda x: x[0] >= 8).values().cache()

        numTraining = training.count()
        numValidation = validation.count()
        numTest = test.count()

        print "Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest)

        # train models and evaluate them on the validation set
        rank = 10
        numIter = 10
        lmbda = 0.1
        model = ALS.train(training, rank, numIter, lmbda)

        # make personalized recommendations
        myRatedStoreIds = set([x[1] for x in myRatings])
        candidates = app.sc.parallelize([s for s in stores.keys().collect() if s not in myRatedStoreIds])
        predictions = model.predictAll(candidates.map(lambda x: (-1, x))).collect()
        recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:20]

        recommendations_id = [row[1] for row in recommendations]

        store_names = stores.filter(lambda (s_id, name): s_id in recommendations_id) \
            .map(lambda (s_id, name): (name)).collect()

        print "Stores recommended for you:"
        for i in xrange(len(recommendations)):
            print ("%2d: %s" % (i + 1, store_names[i])).encode('ascii', 'ignore')

        # clean up
        app.sc.stop()
        return  store_names

app.tasks.register(StoreRecommender)

if __name__ == "__main__":
    # conf = SparkConf().setAppName("recommender")
    # sc = SparkContext(conf = conf)
    main()

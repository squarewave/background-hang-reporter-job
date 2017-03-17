from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from background_hang_reporter_job import main

if __name__ == "__main__":
    conf = SparkConf().setAppName('background_hang_reporter_job')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    main.etl_job(sc, sqlContext)

import datetime
import os
import time

from dotenv import load_dotenv
from py4j.protocol import Py4JJavaError
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

APP_ROOT = os.path.join(os.path.dirname(__file__))  # refers to application_top
load_dotenv(os.path.join(APP_ROOT, '../.env'))

timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


def newsapi_etl():
    spark = SparkSession.builder.appName('news_api_etl').master("local[*]").getOrCreate()
    sc = SparkContext.getOrCreate()

    # bring in all the data from blob storage and add date of etl
    # add credited source and day of data manipulation
    df = spark.read.parquet('./data/data-lake/*/*.parquet')
    df = df \
        .withColumn('process_date', F.unix_timestamp(F.lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast('timestamp')) \
        .withColumn('credited_source', F.lit('newsapi.org').cast('string'))

    # deal with duplicate and null data from the newsapi_job
    df = df.distinct().dropna()

    # log schema for poc
    df.printSchema()
    df.show()

    # write data back into s3 for additional data enrichment and pipeline processes in a scalable environment
    df.repartition(1).write \
        .parquet(f"./data/marts/news-api/{datetime.datetime.today().strftime('%y%M%d-%H%M')}.parquet")

    # 'append' data to mysql database, handle db connection error when container is down
    try:
        df.write.format('jdbc').options(
            url=os.getenv('DB_URL'),
            driver='com.mysql.jdbc.Driver',
            dbtable='newsapi_articles',
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')) \
            .mode('append').save()
    except Py4JJavaError:
        raise ConnectionError('Could not connect to database, check creds or container.')

    spark.stop()

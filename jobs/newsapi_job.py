import datetime
import os

import requests
from dotenv import load_dotenv
from pyspark import SparkContext
from pyspark.sql import SparkSession

APP_ROOT = os.path.join(os.path.dirname(__file__))  # refers to application_top
load_dotenv(os.path.join(APP_ROOT, '../.env'))


def get_articles():
    """
    python requests connects to newsapi.org api and gets json data according to a set of parameters
    :return: json string: headline articles from today to the last 7 days
    """
    parameters = {
        'apiKey': os.getenv('NEWSAPI_KEY'),
        'language': 'en',
        'from': datetime.timedelta(7),
        'to': datetime.datetime.today()
    }

    # handle free max retry error on the api
    try:
        response = requests.get("https://newsapi.org/v2/top-headlines?", params=parameters).json()
        return response['articles']
    except ConnectionError:
        return None


def newsapi_job():
    """
    spark application to clean up and arrange the data accordingly before writing it directly to data lake
    :return: None
    """
    # set up spark job with sc for json parallelization
    spark = SparkSession.builder.appName('news_api').master("local[*]").getOrCreate()
    sc = SparkContext.getOrCreate()

    # get the data and pre-process it for "blob" lake storage
    data = get_articles()
    # ToDo: for the usecase na's should be dropped as the api seems to be sending in null data

    # only write to parquet if there is any data
    if data is not None:
        df = spark.read.json(sc.parallelize(data), multiLine=True, encoding='utf-8') \
            .select('author', 'content', 'description', 'publishedAt', 'source', 'title', 'url', 'urlToImage')

        # write to data-lake (this would ideally be to s3)
        df.repartition(1).write \
            .parquet(f"./data/data-lake/news-api/{datetime.datetime.today().strftime('%y%M%d-%H%M')}.parquet")

    spark.stop()

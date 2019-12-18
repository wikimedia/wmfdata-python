import findspark
findspark.init('/usr/lib/spark2')
from pyspark.sql import SparkSession
import os


# TODO:
# Auto zip and ship juptyer venv with yarn spark job.
# https://wikitech.wikimedia.org/wiki/SWAP#Launching_as_SparkSession_in_a_Python_Notebook



def get_spark_session(master='local', app_name='wmfdata', spark_config={}):
    """
    Returns an existent SparkSession, or a new one if one hasn't yet been created.
    """

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config(
            'spark.driver.extraJavaOptions',
            ' '.join('-D{}={}'.format(k, v) for k, v in {
                'http.proxyHost': 'webproxy.eqiad.wmnet',
                'http.proxyPort': '8080',
                'https.proxyHost': 'webproxy.eqiad.wmnet',
                'https.proxyPort': '8080',
            }.items()))
    )

    for k, v in spark_config.items():
        builder.config(k, v)

    spark = builder.getOrCreate()
    return spark

from pyspark.sql import SparkSession
import sys

def create_spark_session():
    reload(sys)
    sys.setdefaultencoding('utf-8')

    spark = SparkSession.builder.appName('Python questions').config('spark.some.config.option').getOrCreate()

    print('Initializing spark session')

    return spark


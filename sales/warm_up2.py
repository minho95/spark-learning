from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType

print('Warm-up 2:')

spark = SparkSession.builder \
                    .appName('Sales') \
                    .getOrCreate()


products_table = spark.read.parquet('./data/products_parquet')
sales_table = spark.read.parquet('./data/sales_parquet')
sellers_table = spark.read.parquet('./data/sellers_parquet')


sales_table.groupBy(col('date')) \
           .agg(countDistinct(col('product_id')).alias('distinct_products_sold')) \
           .orderBy(col('distinct_products_sold').desc()).show()


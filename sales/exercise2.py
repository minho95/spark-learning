from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType

# Create the Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3g") \
    .appName("Exercise2") \
    .getOrCreate()


products_table = spark.read.parquet("./data/products_parquet")
sales_table = spark.read.parquet("./data/sales_parquet")
sellers_table = spark.read.parquet("./data/sellers_parquet")

sales_table.join(broadcast(sellers_table), sales_table['seller_id'] == sellers_table['seller_id'], 'inner') \
            .withColumn('ratio', sales_table['num_pieces_sold']/sellers_table['daily_target']) \
            .groupBy(sales_table['seller_id']).agg(avg('ratio')).show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row, Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3g") \
    .appName("Exercise3") \
    .getOrCreate()

products_table = spark.read.parquet("./data/products_parquet")
sales_table = spark.read.parquet("./data/sales_parquet")
sellers_table = spark.read.parquet("./data/sellers_parquet")


sales_table = sales_table.groupBy(col('product_id'), col('seller_id')) \
                         .agg(sum('num_pieces_sold').alias('num_pieces_sold'))

window_desc = Window.partitionBy(col('product_id')).orderBy(col('num_pieces_sold').desc())
window_asc = Window.partitionBy(col('product_id')).orderBy(col('num_pieces_sold').asc())

sales_table = sales_table.withColumn('rank_asc', dense_rank().over(window_asc)) \
                         .withColumn('rank_desc', dense_rank().over(window_desc))

single_seller = sales_table.where(col('rank_asc') == col('rank_desc')) \
                           .select(col('product_id').alias('single_seller_product_id'),
                                   col('seller_id').alias('single_seller_seller_id'),
                                   lit('Only seller or multiple sellers with same results').alias('type'))

second_seller = sales_table.where(col('rank_desc') == 2).select(
    col('product_id').alias('second_seller_product_id'), col('seller_id').alias('second_seller_seller_id'),
    lit('Second top seller').alias('type'))

least_seller = sales_table.where(col('rank_asc') == 1).select(
    col('product_id'), col('seller_id'), lit('Least seller').alias('type')) \
    .join(single_seller, (sales_table['seller_id'] == single_seller['single_seller_seller_id']) & (
        sales_table['product_id'] == single_seller['single_seller_product_id']), 'left_anti') \
    .join(second_seller, (sales_table['product_id'] == second_seller['second_seller_seller_id']) & (
        sales_table['product_id'] == second_seller['second_seller_product_id']), 'left_anti')

union_table = least_seller.select(col('product_id'), col('seller_id'), col('type')) \
                          .union(second_seller.select(
                              col('second_seller_product_id').alias('product_id'),
                              col('second_seller_seller_id').alias('seller_id'),
                              col('type'))) \
                          .union(single_seller.select(
                              col('single_seller_product_id').alias('product_id'),
                              col('single_seller_seller_id').alias('seller_id'),
                              col('type')))

union_table.show()

union_table.where(col('product_id') == 0).show()
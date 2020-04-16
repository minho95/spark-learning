from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType

print('Warm-up 1:')

spark = SparkSession.builder \
                    .appName('Sales') \
                    .getOrCreate()


products_table = spark.read.parquet('./data/products_parquet')
sales_table = spark.read.parquet('./data/sales_parquet')
sellers_table = spark.read.parquet('./data/sellers_parquet')


print('Number of orders:', sales_table.count())
print('Number of sellers:', sellers_table.count())
print('Number of products:', products_table.count())

print('Number of products sold at least once:')
sales_table.agg(countDistinct(col('product_id'))).show()

print('Product present in more orders:')
sales_table.groupBy(col('product_id')).agg(
    count('*').alias('cnt')).orderBy(col('cnt').desc()).limit(1).show()

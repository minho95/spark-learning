from pyspark.sql.types import *

tagSchema = StructType([
    StructField('Id', DecimalType(), True),
    StructField('Title', StringType(), True)
])

answerSchema = StructType([
    StructField('Id', DecimalType(), True),
    StructField('OwnerUserId', DecimalType(), True),
    StructField('CreationDate', TimestampType(), True),
    StructField('ParentId', DecimalType(), True),
    StructField('Score', IntegerType(), True),
    StructField('Body', StringType(), True)
])

questionSchema = StructType([
    StructField('Id', DecimalType(), True),
    StructField('OwnerUserId', DecimalType(), True),
    StructField('CreationDate', TimestampType(), True),
    StructField('Score', IntegerType(), True),
    StructField('Title', StringType(), True),
    StructField('Body', StringType(), True)
])
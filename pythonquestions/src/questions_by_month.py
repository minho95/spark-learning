from pyspark.sql.functions import year, month
from pyspark.sql.functions import col

from schemas import questionSchema, answerSchema
from create_spark_session import create_spark_session

spark = create_spark_session()

questionsData = spark.read.option('delimiter', ',') \
                     .csv('./data/Questions.csv', header=False, schema=questionSchema, multiLine=True, escape='"')

questions_by_month = questionsData.withColumn('Month', month('CreationDate')) \
                                  .withColumn('Year', year('CreationDate')) \
                                  .groupBy('Month', 'Year') \
                                  .count().sort(['Year', 'Month'])

questions_by_month.repartition(1) \
         .write.format('csv') \
         .mode('overwrite') \
         .save(path='output/questions_by_month', header=True)

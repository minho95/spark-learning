from pyspark.sql.functions import year
from pyspark.sql.functions import col

from schemas import questionSchema, answerSchema
from create_spark_session import create_spark_session

spark = create_spark_session()

questionsData = spark.read.option('delimiter', ',') \
                     .csv('./data/Questions.csv', header=False, schema=questionSchema, multiLine=True, escape='"')

questions_by_year = questionsData.groupBy(year('CreationDate').alias('year')) \
                                 .count().sort(col('year'))

questions_by_year.repartition(1) \
         .write.format('csv') \
         .mode('overwrite') \
         .save(path='output/questions_by_year', header=True)
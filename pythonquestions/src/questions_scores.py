from pyspark.sql.functions import col

from schemas import questionSchema
from create_spark_session import create_spark_session

spark = create_spark_session()

#question with the highest score

questionsData = spark.read.option('delimiter', ',') \
                     .csv('./data/Questions.csv', header=False, schema=questionSchema, multiLine=True, escape='"')

questions = questionsData.sort(col('Score').desc()).select('Title', 'Score')

questions.repartition(1) \
         .write.format('csv') \
         .mode('overwrite') \
         .save(path='output/most_popular_questions', header=True)

from pyspark.sql.functions import col

from schemas import answerSchema, questionSchema
from create_spark_session import create_spark_session

spark = create_spark_session()

answersData = spark.read.option('delimiter', ',') \
                     .csv('./data/Answers.csv', header=False, schema=answerSchema, multiLine=True, escape='"')


questionsData = spark.read.option('delimiter', ',') \
                     .csv('./data/Questions.csv', header=False, schema=questionSchema, multiLine=True, escape='"')

answers = answersData.withColumnRenamed('Id', 'AnswerId') \
                 .withColumnRenamed('Score', 'AnswerScore') \
                 .withColumnRenamed('Body', 'AnswerBody')

joinedAnswers = answers.join(questionsData, answers.ParentId == questionsData.Id) \
                       .sort(col('AnswerScore').desc()) \
                       .limit(10)

joinedAnswers.select('AnswerId', 'ParentId', 'AnswerScore', 'Title') \
             .repartition(1) \
             .write.format('csv') \
             .mode('overwrite') \
             .save(path='output/most_popular_answers', header=True)

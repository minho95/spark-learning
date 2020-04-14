from pyspark.sql.functions import col

from schemas import questionSchema, answerSchema
from create_spark_session import create_spark_session

spark = create_spark_session()


questionsData = spark.read.option('delimiter', ',') \
                     .csv('./data/Questions.csv', header=False, schema=questionSchema, multiLine=True, escape='"')

answersData = spark.read.option('delimiter', ',') \
                     .csv('./data/Answers.csv', header=False, schema=answerSchema, multiLine=True, escape='"')

joinedWithAnswers = questionsData.select('Id', 'Title') \
                                 .join(answersData, questionsData.Id == answersData.ParentId, how='left_anti')

joinedWithAnswers.repartition(1) \
         .write.format('csv') \
         .mode('overwrite') \
         .save(path='output/not_answered_questions', header=True)
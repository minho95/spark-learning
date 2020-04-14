from pyspark.sql.functions import col

from schemas import questionSchema, answerSchema
from create_spark_session import create_spark_session

spark = create_spark_session()

questionsData = spark.read.option('delimiter', ',') \
                .csv('./data/Questions.csv', header=False, schema=questionSchema, multiLine=True, escape='"')

answersData = spark.read.option('delimiter', ',') \
                .csv('./data/Answers.csv', header=False, schema=answerSchema, multiLine=True, escape='"')


groupedAnswers = answersData.groupBy('ParentId') \
                            .count()

groupedAnswers = groupedAnswers.join(questionsData, groupedAnswers.ParentId == questionsData.Id) \
                               .select('ParentId', 'count', 'Title', 'Score') \
                               .sort(col('count').desc()) \
                               .withColumnRenamed('ParentId', 'QuestionId') \
                               .withColumnRenamed('count', 'AnswersCount')
                               
groupedAnswers.repartition(1) \
            .write.format('csv') \
            .mode('overwrite') \
            .save(path='output/most_answers', header=True)

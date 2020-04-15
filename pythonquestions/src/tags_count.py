from pyspark.sql.functions import col

from schemas import tagSchema, questionSchema, answerSchema
from create_spark_session import create_spark_session

spark = create_spark_session()

#tags counting

tagsData = spark.read.option('delimiter', ',') \
                .csv('./data/Tags.csv', header=False, schema=tagSchema)

tags = tagsData.filter(tagsData['Title'] != 'python')

groupedTags = tags.groupBy('Title').count().sort(col('count').desc()) \
                  .withColumnRenamed('count', 'Count')

groupedTags.repartition(1) \
            .write.format('csv') \
            .mode('overwrite') \
            .save(path='output/tags_count', header=True)

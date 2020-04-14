from pyspark.sql.functions import col

from schemas import tagSchema
from create_spark_session import create_spark_session

spark = create_spark_session()

#tags counting

tagsData = spark.read.option('delimiter', ',') \
                .csv('./data/Tags.csv', header=False, schema=tagSchema)

tags1 = tagsData.filter(tagsData['Title'] != 'python')

tags2 = tags1.withColumnRenamed('Title', 'SecondTitle') \
             .withColumnRenamed('Id', 'SecondId')

joinedTags = tags1.join(tags2, (tags1.Id == tags2.SecondId) & (tags1.Title != tags2.SecondTitle))

groupedTags = joinedTags.groupBy('Title', 'SecondTitle') \
                        .count().sort(col('count').desc())

groupedTags.repartition(1) \
           .write.format('csv') \
           .mode('overwrite') \
           .save(path='output/tags_pairs_count', header=True)

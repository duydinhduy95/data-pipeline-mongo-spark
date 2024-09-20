from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import regexp_extract

import re
from pyspark.sql.types import DateType, IntegerType, ArrayType, StringType
from pyspark.sql.functions import col, when, expr, explode, count, udf, sum, to_date

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('MyApp') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', 'mongodb://root:root@mongodb:27017/asm2_db?authSource=admin') \
        .getOrCreate()

    question_df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("collection", "Questions")\
        .load()
    # question_df.show()

    answer_df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option('collection', 'Answers') \
        .load()
    # answer_df.show()

    # preprocessing question_df, convert ClosedDate, CreationDate to DateType, change NA to null in OwnerUserId and change datatype to IntegerType
    changed_question_df = question_df.withColumn("ClosedDate", col("ClosedDate").cast(DateType())) \
        .withColumn("CreationDate", col("CreationDate").cast(DateType())) \
        .withColumn("OwnerUserId", when(question_df.OwnerUserId == "NA", 'null') \
                    .otherwise(question_df.OwnerUserId)) \
        .withColumn("OwnerUserId", col("OwnerUserId").cast(IntegerType()))
    # changed_question_df.show()

    # preprocessing answer_df, convert CreationDate to DateType, change NA to null in OwnerUserId and change datatype to IntegerType
    changed_answer_df = answer_df.withColumn("CreationDate", col("CreationDate").cast(DateType())) \
        .withColumn("OwnerUserId", when(answer_df.OwnerUserId == "NA", "null") \
                    .otherwise(answer_df.OwnerUserId)) \
        .withColumn("OwnerUserId", col("OwnerUserId").cast(IntegerType()))


    # join answer and question dataframe
    join_expr = changed_question_df.Id == changed_answer_df.ParentId
    join_df = changed_question_df.join(changed_answer_df, join_expr, "inner").drop(changed_answer_df.Id)

    # group by and count number of answers for each questions
    count_join_df = join_df.groupBy("Id").agg(count("*").alias("NumOfAnswers"))

    count_join_df = count_join_df.orderBy('NumOfAnswers', ascending=False)

    count_join_df.coalesce(1).toPandas().to_csv('/opt/airflow/data/output.csv', index=False)
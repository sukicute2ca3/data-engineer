from asyncio import current_task
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
spark = SparkSession.builder.appName('Practise').getOrCreate()
circuits_df = spark.read.csv(
    '/Users/mac/Documents/dataengineer/f1db_csv/circuits.csv', header=True, inferSchema=True)


# circuits_df = spark.read.option('header', True) \
#     .option('inferSchema', True) \
#     .csv('/Users/mac/Documents/dataengineer/f1db_csv/circuits.csv')
# print(circuits_df.describe().show())


# # rename
# circuits_df = spark.read.option('header', True).option('inforSchame', True).csv(
#     '/Users/mac/Documents/dataengineer/f1db_csv/circuits.csv')
# print(circuits_df.describe().show())
# circuits_df = circuits_df.withColumnRenamed(
#     'circuitId', 'Id').withColumnRenamed('name', 'Name')
# print(circuits_df.show())

# circuits_final_df = circuits_df.withColumn(
#     'ingestion_date', current_timestamp()).withColumn('env', lit('Production'))
# print(circuits_final_df.show())

print(circuits_df.show())

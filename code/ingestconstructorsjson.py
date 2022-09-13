import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


spark = SparkSession.builder.master(
    'local[1]').appName('Practise').getOrCreate()

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
constructor_df = spark.read.schema(constructors_schema).option('multiLine', True).json(
    "/Users/mac/Documents/dataengineer/f1db_csv/constructors.json")
constructor_df = constructor_df.withColumnRenamed(
    'constructorId', 'constructor_Id')
constructor_df = constructor_df.drop('url')
print(constructor_df.show())

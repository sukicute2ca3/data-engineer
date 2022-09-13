import pandas as pd

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

spark = SparkSession.builder.appName("Practise").getOrCreate()
races_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                  StructField('year', IntegerType(), True),
                                  StructField('round', IntegerType(), True),
                                  StructField(
                                      'circuitId', IntegerType(), True),
                                  StructField('name', StringType(), True),
                                  StructField('date', DateType(), True),
                                  StructField('time', StringType(), True),
                                  StructField('url', StringType(), True)
                                  ])
race_df = spark.read.option('header', True).schema(races_schema).csv(
    '/Users/mac/Documents/dataengineer/f1db_csv/races.csv')


circuits_df = spark.read.csv(
    '/Users/mac/Documents/dataengineer/f1db_csv/circuits.csv', header=True, inferSchema=True)

race_circuits_df = circuits_df.join(race_df, circuits_df.circuitId == race_df.circuitId, 'inner').select(circuits_df.name, circuits_df.location,
                                    circuits_df.country, race_df.name, race_df.round)
# print(race_df.show())
# print(circuits_df.show())
print(race_circuits_df.show())
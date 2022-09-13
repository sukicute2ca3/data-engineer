from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

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
spark = SparkSession.builder.appName('DataFrame').getOrCreate()

races_df = spark.read.option('header', True).schema(races_schema).csv(
    '/Users/mac/Documents/dataengineer/f1db_csv/races.csv')


#
races_with_timestamp_df = races_df.withColumn(
    'ingestion_date', current_timestamp()).withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId')
                                                   .alias('circuit_id'),
                                                   col('name'), col('ingestion_date'), col('race_timestamp'))
print(races_selected_df.show())

# dùng hàm filter
# # cách 1
# races_selected_df = races_selected_df.filter(
#     races_selected_df.race_id < 5).collect()


# # cách 2
# races_selected_df = races_selected_df.filter(
#     'race_id > 5').filter('race_year = 2019').collect()

#  cách 3
races_selected_df = races_selected_df.filter(
    races_selected_df['race_id'] == 5).collect()


for i in range(len(races_selected_df)):
    print(races_selected_df[i])

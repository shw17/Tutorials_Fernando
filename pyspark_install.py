# in the terminal: pip install pyspark 

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Tutorial3').getOrCreate()
df = spark.read.csv('/Users/shuwen/Desktop/spam_base.csv',header=True)
print(type(df))
print(df.head())
print(df.printSchema())

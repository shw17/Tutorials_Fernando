# in the terminal: pip install pyspark 

import pyspark
from pyspark import SparkFiles
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('installpy').getOrCreate()
url = "https://raw.githubusercontent.com/shw17/Tutorials_Fernando/master/spam_base.csv"
spark.sparkContent.addFile(url)
df = spark.read.csv(SparkFiles.get("spam_base.csv"),inferSchema=True, header=True)
print(type(df))
print(df.head())
print(df.printSchema())

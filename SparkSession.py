from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("grouping").getOrCreate()
df=spark.read.csv("custormer.csv",inferSchema=True,header=True)
df.show()
grouped=df.groupBy("dept").count().orderBy("count",ascending=False)
grouped.show()
